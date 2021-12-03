use crate::{
    application::conf::logger::ArconLogger,
    application::AssembledApplication,
    buffer::event::PoolInfo,
    control_plane::distributed::GlobalNodeId,
    data::{flight_serde::FlightSerde, ArconMessage, ArconType, NodeID},
    dataflow::{
        api::{OperatorBuilder, ParallelSourceBuilder, SourceBuilder, SourceBuilderType},
        conf::SourceConf,
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
use std::{any::Any, path::PathBuf, sync::Arc};

/*pub type SourceConstructor = Box<
    dyn Fn(
        Vec<Arc<dyn Any + Send + Sync>>,
        ChannelKind,
        &mut KompactSystem,
    ) -> Arc<dyn AbstractComponent<Message = SourceEvent>>,
>; */
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

pub trait NodeFactory {
    fn build_nodes(
        &mut self,
        node_ids: Vec<GlobalNodeId>,
        in_channels: Vec<NodeID>,
        components: ErasedComponents,
        paths: Vec<ActorPath>,
        application: &mut AssembledApplication,
    ) -> Vec<(GlobalNodeId, ErasedComponent)>;

    fn set_channel_kind(&mut self, channel_kind: ChannelKind);
}

pub trait SourceFactory {
    fn build_source(
        &mut self,
        components: ErasedComponents,
        paths: Vec<ActorPath>,
        application: &mut AssembledApplication,
    ) -> ErasedSourceManager;

    fn set_channel_kind(&mut self, channel_kind: ChannelKind);
}

impl<OP: Operator + 'static, B: Backend> NodeFactory for NodeConstructor<OP, B> {
    fn build_nodes(
        &mut self,
        node_ids: Vec<GlobalNodeId>,
        in_channels: Vec<NodeID>,
        components: ErasedComponents,
        paths: Vec<ActorPath>,
        application: &mut AssembledApplication,
    ) -> Vec<(GlobalNodeId, ErasedComponent)> {
        // Initialize state and manager
        self.init_state_dir();
        self.init_node_manager(application, &in_channels);

        for node_id in node_ids {
            // Create the Nodes arguments
            let node_descriptor = format!("{}_{}", self.descriptor, node_id.node_id.id);
            let backend = self.create_backend(node_descriptor.clone());
            let channel_strategy = channel_strategy(
                components.clone(),
                paths.clone(),
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
                NodeState::new(node_id.node_id, in_channels.clone(), backend.clone()),
                backend.clone(),
                self.logger.clone(),
                application.epoch_manager().clone(),
                #[cfg(all(feature = "hardware_counters", target_os = "linux", not(test)))]
                builder.conf.perf_events.clone(),
                node_id.clone(),
            );

            // Create the node and connect it to the NodeManager
            self.create_node_component(application, node);
        }
        // Start NodeManager
        let manager_comp = self
            .node_manager
            .as_ref()
            .expect("NodeManager must be initialized to spawn node");
        application
            .ctrl_system()
            .start_notify(&manager_comp)
            .wait_timeout(std::time::Duration::from_millis(2000))
            .expect("Failed to start NodeManager");

        // Fetch all created Nodes on this NodeManager and return them as Erased
        // for the next stage..
        manager_comp.on_definition(|cd| {
            cd.nodes
                .iter()
                .map(|(id, (comp, _))| (id.clone(), Arc::new(comp.clone()) as ErasedComponent))
                .collect()
        })
    }

    fn set_channel_kind(&mut self, channel_kind: ChannelKind) {
        self.channel_kind = channel_kind;
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
    ) -> NodeConstructor<OP, B> {
        NodeConstructor {
            descriptor,
            state_dir,
            logger,
            channel_kind: ChannelKind::default(),
            builder,
            node_manager: None,
        }
    }

    fn create_node_component(&mut self, application: &mut AssembledApplication, node: Node<OP, B>) {
        let node_id = node.node_id;
        let node_comp = application.data_system().create(|| node);
        let required_ref: RequiredRef<NodeManagerPort> = node_comp.required_ref();

        let manager_comp = self
            .node_manager
            .as_ref()
            .expect("NodeManager must be initialized to spawn node");
        biconnect_components::<NodeManagerPort, _, _>(&manager_comp, &node_comp).expect("fail");

        let node_comp: Arc<dyn AbstractComponent<Message = ArconMessage<OP::IN>>> = node_comp;

        application
            .data_system()
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
        self.builder
            .create_backend(node_dir, node_descriptor.clone())
    }

    /// Initializes things like state directory and NodeManager
    fn init_node_manager(
        &mut self,
        application: &mut AssembledApplication,
        in_channels: &Vec<NodeID>,
    ) {
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

pub(crate) struct SourceConstructor<S: Source + 'static, B: Backend> {
    descriptor: String,
    builder_type: SourceBuilderType<S, B>,
    backend: Arc<B>,
    watermark_interval: u64,
    time: ArconTime,
    channel_kind: ChannelKind,
}

impl<S: Source + 'static, B: Backend> SourceConstructor<S, B> {
    pub(crate) fn new(
        descriptor: String,
        builder_type: SourceBuilderType<S, B>,
        backend: Arc<B>,
        watermark_interval: u64,
        time: ArconTime,
    ) -> Self {
        SourceConstructor {
            descriptor,
            builder_type,
            backend,
            watermark_interval,
            time,
            channel_kind: ChannelKind::default(),
        }
    }

    // Source Manager needs to be (re-)inserted after calling this function
    fn create_source_manager(
        &mut self,
        application: &mut AssembledApplication,
    ) -> Arc<Component<SourceManager<B>>> {
        let manager = SourceManager::new(
            self.descriptor.clone(),
            self.time,
            self.watermark_interval,
            application.epoch_manager(),
            self.backend.clone(),
            application.app.arcon_logger.clone(),
        );
        application.ctrl_system().create(|| manager)
    }

    fn start_source_manager(
        &mut self,
        source_manager: &Arc<Component<SourceManager<B>>>,
        application: &mut AssembledApplication,
    ) {
        let source_ref: ActorRefStrong<SourceEvent> =
            source_manager.actor_ref().hold().expect("fail");
        // Set source reference at the EpochManager
        if let Some(epoch_manager) = &application.runtime.epoch_manager {
            epoch_manager.on_definition(|cd| {
                cd.source_manager = Some(source_ref);
            });
        }

        application
            .ctrl_system()
            .start_notify(&source_manager)
            .wait_timeout(std::time::Duration::from_millis(2000))
            .expect("Failed to start SourceManager");
    }
}

impl<S: Source + 'static, B: Backend> SourceFactory for SourceConstructor<S, B> {
    fn build_source(
        &mut self,
        components: ErasedComponents,
        paths: Vec<ActorPath>,
        application: &mut AssembledApplication,
    ) -> ErasedSourceManager {
        let source_manager = self.create_source_manager(application);

        match &self.builder_type {
            SourceBuilderType::Single(builder) => {
                let source_cons = builder.constructor.clone();
                let source_conf = builder.conf.clone();
                let source_index = 0;
                let source = source_cons(self.backend.clone());
                create_source_node(
                    application,
                    source_index,
                    components,
                    paths,
                    self.channel_kind,
                    source,
                    source_conf,
                    &source_manager,
                );
            }
            SourceBuilderType::Parallel(builder) => {
                let source_cons = builder.constructor.clone();
                let parallelism = builder.parallelism;
                for source_index in 0..builder.parallelism {
                    let source_conf = builder.conf.clone();
                    let source = source_cons(self.backend.clone(), source_index, parallelism); // todo
                    create_source_node(
                        application,
                        source_index,
                        components.clone(),
                        paths.clone(),
                        self.channel_kind,
                        source,
                        source_conf,
                        &source_manager,
                    );
                }
            }
        }
        self.start_source_manager(&source_manager, application);
        source_manager
    }

    fn set_channel_kind(&mut self, channel_kind: ChannelKind) {
        self.channel_kind = channel_kind;
    }
}
/*
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
*/
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
