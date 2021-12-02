use crate::{
    application::conf::logger::ArconLogger,
    application::AssembledApplication,
    buffer::event::PoolInfo,
    control_plane::distributed::GlobalNodeId,
    data::{ArconMessage, ArconType, NodeID, flight_serde::FlightSerde},
    dataflow::{
        api::{OperatorBuilder, SourceBuilderType},
        conf::{ParallelismStrategy, SourceConf},
        dfg::ChannelKind,
    },
    manager::{
        node::{NodeManager, NodeManagerPort},
        source::{SourceManager, SourceManagerPort},
    },
    stream::{
        channel::{
            strategy::{forward::Forward, keyed::Keyed, *},
            Channel,
        },
        node::{
            source::{SourceEvent, SourceNode},
            Node, NodeState,
        },
        operator::Operator,
        source::Source,
        time::ArconTime,
    },
};
use arcon_state::Backend;
use kompact::{
    component::AbstractComponent,
    prelude::{
        biconnect_components, biconnect_ports, ActorRefFactory, ActorRefStrong, KompactSystem,
        RequiredRef, *,
    },
};
use std::{any::Any, convert::TryInto, path::PathBuf, sync::Arc};

pub type SourceConstructor = Box<
    dyn Fn(
        Vec<Arc<dyn Any + Send + Sync>>,
        ChannelKind,
        &mut KompactSystem,
    ) -> Arc<dyn AbstractComponent<Message = SourceEvent>>,
>;
pub type ErasedSourceManager = Arc<dyn AbstractComponent<Message = SourceEvent>>;

pub type ErasedComponent = Arc<dyn Any + Send + Sync>;
pub type ErasedComponents = Vec<ErasedComponent>;

fn channel_strategy<OUT: ArconType>(
    mut components: ErasedComponents,
    paths: Vec<ActorPath>,
    node_id: NodeID,
    pool_info: PoolInfo,
    max_key: u64,
    channel_kind: ChannelKind,
) -> ChannelStrategy<OUT> {
    match channel_kind {
        ChannelKind::Forward => {
            assert_eq!(components.len(), 1, "Expected a single component target");
            let target_node = components
                .remove(0)
                .downcast::<Arc<dyn AbstractComponent<Message = ArconMessage<OUT>>>>()
                .unwrap();
            let actor_ref = target_node.actor_ref().hold().expect("failed to fetch");
            ChannelStrategy::Forward(Forward::new(Channel::Local(actor_ref), node_id, pool_info))
        }
        ChannelKind::Keyed => {
            let mut channels = Vec::new();
            for component in components {
                let target_node = component
                    .downcast::<Arc<dyn AbstractComponent<Message = ArconMessage<OUT>>>>()
                    .unwrap();
                let actor_ref = target_node.actor_ref().hold().expect("failed to fetch");
                let channel = Channel::Local(actor_ref);
                channels.push(channel);
            }
            for path in paths {
                channels.push(Channel::Remote(path, FlightSerde::Reliable));
            }
            ChannelStrategy::Keyed(Keyed::new(max_key, channels, node_id, pool_info))
        }
        ChannelKind::Console => ChannelStrategy::Console,
        ChannelKind::Mute => ChannelStrategy::Mute,
        _ => unimplemented!(),
    }
}

pub(crate) struct NodeConstructor<OP: Operator + 'static, B: Backend> {
    descriptor: String,
    state_dir: PathBuf,
    logger: ArconLogger,
    channel_kind: ChannelKind,
    builder: Arc<OperatorBuilder<OP, B>>,
    node_manager: Option<Arc<Component<NodeManager<OP, B>>>>,
}

impl<OP: Operator + 'static, B: Backend> NodeConstructor<OP, B> {
    pub fn new(
        descriptor: String,
        state_dir: PathBuf,
        builder: Arc<OperatorBuilder<OP, B>>,
        logger: ArconLogger,
        channel_kind: ChannelKind,
    ) -> NodeConstructor<OP, B> {
        NodeConstructor{
            descriptor,
            state_dir,
            logger,
            channel_kind,
            builder,
            node_manager: None,
        }
    }

    pub fn build_nodes(&mut self,
        node_ids: Vec<GlobalNodeId>,
        in_channels: Vec<NodeID>,
        components: ErasedComponents,
        paths: Vec<ActorPath>,
        application: &mut AssembledApplication
    ) -> ErasedComponents {
        // Initialize state and manager
        self.init_state_dir();
        self.init_node_manager(application, &in_channels);

        let nodes = node_ids.iter().map(|node_id| {
            // Create the Nodes arguments
            let node_descriptor = format!("{}_{}", self.descriptor, node_id.id);
            let backend = self.create_backend(node_descriptor.clone());
            let channel_strategy = channel_strategy(
                components,
                paths,
                node_id.node_id,
                application.app.get_pool_info().clone(),
                application.app.conf.max_key,
                self.channel_kind,
            );

            // Build the Node
            let node = Node::new(
                node_descriptor,
                channel_strategy,
                self.builder.operator.clone()(),
                self.builder.state.clone()(backend.clone()),
                NodeState::new(node_id, in_channels.clone(), backend.clone()),
                backend.clone(),
                self.logger,
                application.epoch_manager().clone(),
                #[cfg(all(
                    feature = "hardware_counters",
                    target_os = "linux",
                    not(test)
                ))]
                builder.conf.perf_events.clone(),
                node_id,
            );

            // Create the node and connect it to the NodeManager
            self.create_node_component(application, node);
        });
        // Start NodeManager
        let manager_comp = self.node_manager.expect("NodeManager must be initialized to spawn node");
        application.ctrl_system()
            .start_notify(&manager_comp)
            .wait_timeout(std::time::Duration::from_millis(2000))
            .expect("Failed to start NodeManager");

        // Fetch all created Nodes on this NodeManager and return them as Erased
        // for the next stage..
        manager_comp.on_definition(|cd| {
            cd.nodes
                .values()
                .map(|(comp, _)| Arc::new(comp.clone()) as ErasedComponent)
                .collect()
        })
    }

    fn start_node_manager(&mut self) {
        let manager_comp = self.node_manager.expect("NodeManager must be initialized to spawn node");

    }
//  -> Arc<dyn AbstractComponent<Message = ArconMessage<OP::IN>>> 
    fn create_node_component(&mut self, application: &mut AssembledApplication, node: Node<OP, B>) {
        let node_id = node.node_id.node_id;
        let node_comp = application.data_system().create(|| node);
        let required_ref: RequiredRef<NodeManagerPort> = node_comp.required_ref();
        
        let manager_comp = self.node_manager.expect("NodeManager must be initialized to spawn node");
        biconnect_components::<NodeManagerPort, _, _>(&manager_comp, &node_comp)
            .expect("fail");

        let node_comp: Arc<dyn AbstractComponent<Message = ArconMessage<OP::IN>>> =
            node_comp;

        application.data_system()
            .start_notify(&node_comp)
            .wait_timeout(std::time::Duration::from_millis(2000))
            .expect("Failed to start Node Component");

        manager_comp.on_definition(|cd| {
            // Insert the created Node into the NodeManager
            cd.nodes.insert(node_id, (node_comp.clone(), required_ref));
        });
        // node_comp
    }

    fn create_backend(&mut self, node_descriptor: String) -> Arc<B> {
        let mut node_dir = self.state_dir.clone();
        node_dir.push(&node_descriptor);
        self.builder.create_backend(node_dir, node_descriptor.clone())
    }

    /// Initializes things like state directory and NodeManager
    fn init_node_manager(&mut self, application: &mut AssembledApplication, in_channels: &Vec<NodeID>) {
        if self.node_manager.is_none() {
            // Define the NodeManager
            let manager = NodeManager::<OP, B>::new(
                self.descriptor.clone(),
                application.data_system().clone(),
                in_channels.clone(),
                self.logger.clone(),
                self.builder.clone(),
            );
            // Create the actual NodeManager component
            let manager_comp = application.ctrl_system().create(|| manager);

            // Connect NodeManager to the SnapshotManager of the application
            application.snapshot_manager().on_definition(|scd| {
                manager_comp.on_definition(|cd| {
                    biconnect_ports(&mut scd.manager_port, &mut cd.snapshot_manager_port);
                });
            });
            self.node_manager = Some(manager_comp);
        }
    }

    fn init_state_dir(&mut self) {
        // Ensure there's a state_directory
        std::fs::create_dir_all(&self.state_dir).unwrap();
    }
}
/*
pub struct NodeConstructor {
    constructor: Box<
        dyn Fn(
            NodeID,
            Vec<NodeID>,
            ErasedComponents,
            Vec<ActorPath>,
            ChannelKind,
            &mut AssembledApplication,
        ) -> ErasedComponents,
    >,
}

impl NodeConstructor {
    pub(crate) fn build(
        &self,
        node_id: NodeID,
        in_channels: Vec<NodeID>,
        components: ErasedComponents,
        paths: Vec<ActorPath>,
        channel_kind: ChannelKind,
        application: &mut AssembledApplication,
    ) -> ErasedComponents {
        (self.constructor)(node_id, in_channels, components, paths, channel_kind, application)
    }

    pub(crate) fn new<OP: Operator + 'static, B: Backend>(
        descriptor: String,
        state_dir: PathBuf,
        builder: Arc<OperatorBuilder<OP, B>>,
        logger: ArconLogger,
    ) -> NodeConstructor {
        NodeConstructor {
            constructor: Box::new(
                move |node_id: NodeID,
                      in_channels: Vec<NodeID>,
                      components: ErasedComponents,
                      paths: Vec<ActorPath>,
                      channel_kind: ChannelKind,
                      app: &mut AssembledApplication| {
                    let epoch_manager_ref = app.epoch_manager();

                    // How many instances of this Operator we are initially creating
                    let instances = match builder.conf.parallelism_strategy {
                        ParallelismStrategy::Static(s) => s,
                        _ => panic!("Managed ParallelismStrategy not supported yet"),
                    };

                    let max_key = app.app.conf.max_key as usize;

                    // create base dir
                    std::fs::create_dir_all(&state_dir).unwrap();

                    #[cfg(all(feature = "hardware_counters", target_os = "linux", not(test)))]
                    let perf_events = builder.conf.perf_events.clone();
                    // Fetch the Operator constructor from the builder
                    let operator = builder.operator.clone();
                    // Fetch the Operator state constructor from the builder
                    let operator_state = builder.state.clone();

                    // Define the NodeManager
                    let manager = NodeManager::<OP, B>::new(
                        descriptor.clone(),
                        app.data_system().clone(),
                        in_channels.clone(),
                        logger.clone(),
                        builder.clone(),
                    );
                    // Create the actual NodeManager component
                    let manager_comp = app.ctrl_system().create(|| manager);

                    // Connect NodeManager to the SnapshotManager of the app
                    app.snapshot_manager().on_definition(|scd| {
                        manager_comp.on_definition(|cd| {
                            biconnect_ports(&mut scd.manager_port, &mut cd.snapshot_manager_port);
                        });
                    });

                    // Fetch PoolInfo object that ChannelStrategies use to organise their buffers
                    let pool_info = app.app.get_pool_info();

                    // Create `instances` number of Nodes and add them into the NodeManager
                    for (curr_node_id, _) in (0..instances).enumerate() {
                        let node_descriptor = format!("{}_{}", descriptor, curr_node_id);
                        let node_id = NodeID::new(curr_node_id.try_into().unwrap());
                        let mut node_dir = state_dir.clone();
                        node_dir.push(&node_descriptor);
                        let backend = builder.create_backend(node_dir, node_descriptor.clone());

                        let node = Node::new(
                            node_descriptor,
                            channel_strategy(
                                components.clone(),
                                paths.clone(),
                                node_id,
                                pool_info.clone(),
                                max_key as u64,
                                channel_kind,
                            ),
                            operator(),
                            operator_state(backend.clone()),
                            NodeState::new(node_id, in_channels.clone(), backend.clone()),
                            backend.clone(),
                            app.app.arcon_logger.clone(),
                            epoch_manager_ref.clone(),
                            #[cfg(all(
                                feature = "hardware_counters",
                                target_os = "linux",
                                not(test)
                            ))]
                            perf_events.clone(),
                            node_id,
                        );

                        let node_comp = app.data_system().create(|| node);
                        let required_ref: RequiredRef<NodeManagerPort> = node_comp.required_ref();
                        biconnect_components::<NodeManagerPort, _, _>(&manager_comp, &node_comp)
                            .expect("fail");

                        let node_comp: Arc<dyn AbstractComponent<Message = ArconMessage<OP::IN>>> =
                            node_comp;

                        app.data_system()
                            .start_notify(&node_comp)
                            .wait_timeout(std::time::Duration::from_millis(2000))
                            .expect("Failed to start Node Component");

                        manager_comp.on_definition(|cd| {
                            // Insert the created Node into the NodeManager
                            cd.nodes.insert(node_id, (node_comp, required_ref));
                        });
                    }
                    // Start NodeManager
                    app.ctrl_system()
                        .start_notify(&manager_comp)
                        .wait_timeout(std::time::Duration::from_millis(2000))
                        .expect("Failed to start NodeManager");

                    // Fetch all created Nodes on this NodeManager and return them as Erased
                    // for the next stage..
                    let nodes: ErasedComponents = manager_comp.on_definition(|cd| {
                        cd.nodes
                            .values()
                            .map(|(comp, _)| Arc::new(comp.clone()) as ErasedComponent)
                            .collect()
                    });

                    nodes
                },
            ),
        }
    }
}
*/

pub struct SourceManagerConstructor {
    constructor: Box<
        dyn Fn(
            ErasedComponents,
            Vec<ActorPath>,
            ChannelKind,
            &mut AssembledApplication,
        ) -> ErasedSourceManager,
    >,
}

impl SourceManagerConstructor {
    pub(crate) fn build(
        &self,
        components: ErasedComponents,
        paths: Vec<ActorPath>,
        channel_kind: ChannelKind,
        application: &mut AssembledApplication,
    ) -> ErasedSourceManager {
        (self.constructor)(components, paths, channel_kind, application)
    }

    pub(crate) fn new<S: Source + 'static, B: Backend>(
        descriptor: String,
        builder_type: SourceBuilderType<S, B>,
        backend: Arc<B>,
        watermark_interval: u64,
        time: ArconTime,
    ) -> Self {
        SourceManagerConstructor {
            constructor: Box::new(
                move |components: ErasedComponents,
                      paths: Vec<ActorPath>,
                      channel_kind: ChannelKind,
                      app: &mut AssembledApplication| {
                    let epoch_manager_ref = app.epoch_manager();

                    let manager = SourceManager::new(
                        descriptor.clone(),
                        time,
                        watermark_interval,
                        epoch_manager_ref,
                        backend.clone(),
                        app.app.arcon_logger.clone(),
                    );
                    let source_manager_comp = app.ctrl_system().create(|| manager);

                    match &builder_type {
                        SourceBuilderType::Single(builder) => {
                            let source_cons = builder.constructor.clone();
                            let source_conf = builder.conf.clone();
                            let source_index = 0;
                            let source = source_cons(backend.clone());
                            create_source_node(
                                app,
                                source_index,
                                components.clone(),
                                paths,
                                channel_kind,
                                source,
                                source_conf,
                                &source_manager_comp,
                            );
                        }
                        SourceBuilderType::Parallel(builder) => {
                            let source_cons = builder.constructor.clone();
                            let parallelism = builder.parallelism;
                            for source_index in 0..builder.parallelism {
                                let source_conf = builder.conf.clone();
                                let source =
                                    source_cons(backend.clone(), source_index, parallelism); // todo
                                create_source_node(
                                    app,
                                    source_index,
                                    components.clone(),
                                    paths.clone(),
                                    channel_kind,
                                    source,
                                    source_conf,
                                    &source_manager_comp,
                                );
                            }
                        }
                    }
                    let source_ref: ActorRefStrong<SourceEvent> =
                        source_manager_comp.actor_ref().hold().expect("fail");

                    // Set source reference at the EpochManager
                    if let Some(epoch_manager) = &app.runtime.epoch_manager {
                        epoch_manager.on_definition(|cd| {
                            cd.source_manager = Some(source_ref);
                        });
                    }

                    app.ctrl_system()
                        .start_notify(&source_manager_comp)
                        .wait_timeout(std::time::Duration::from_millis(2000))
                        .expect("Failed to start SourceManager");

                    source_manager_comp
                },
            ),
        }
    }
}

// helper function to create source node..
fn create_source_node<S, B>(
    app: &mut AssembledApplication,
    source_index: usize,
    components: ErasedComponents,
    paths: Vec<ActorPath>,
    channel_kind: ChannelKind,
    source: S,
    source_conf: SourceConf<S::Item>,
    source_manager_comp: &Arc<Component<SourceManager<B>>>,
) where
    S: Source,
    B: Backend,
{
    let pool_info = app.app.get_pool_info();
    let max_key = app.app.conf.max_key;
    let channel_strategy = channel_strategy(
        components.clone(),
        paths,
        NodeID::new(source_index as u32),
        pool_info,
        max_key,
        channel_kind,
    );
    let source_node = SourceNode::new(
        source_index,
        source,
        source_conf,
        channel_strategy,
        app.app.arcon_logger.clone(),
    );
    let source_node_comp = app.data_system().create(|| source_node);

    app.data_system()
        .start_notify(&source_node_comp)
        .wait_timeout(std::time::Duration::from_millis(2000))
        .expect("Failed to start Source Node");

    biconnect_components::<SourceManagerPort, _, _>(source_manager_comp, &source_node_comp)
        .expect("failed to biconnect components");

    let source_node_comp_dyn: Arc<dyn AbstractComponent<Message = SourceEvent>> = source_node_comp;

    source_manager_comp.on_definition(|cd| {
        cd.add_source(source_node_comp_dyn);
    });
}
