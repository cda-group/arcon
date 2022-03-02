use crate::{
    application::Application,
    buffer::event::PoolInfo,
    data::{flight_serde::FlightSerde, ArconMessage, ArconType, NodeID},
    dataflow::{
        builder::{KeyBuilder, OperatorBuilder, SourceBuilderType},
        conf::SourceConf,
        dfg::{ChannelKind, GlobalNodeId},
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
            debug::DebugNode,
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
        biconnect_components, biconnect_ports, ActorRefFactory, ActorRefStrong, RequiredRef, *,
    },
};
use std::{any::Any, path::PathBuf, rc::Rc, sync::Arc};

pub type ErasedSourceManager = Arc<dyn AbstractComponent<Message = SourceEvent>>;

pub type ErasedComponent = Arc<dyn Any + Send + Sync>;
pub type ErasedComponents = Vec<ErasedComponent>;

fn channel_strategy<T: ArconType>(
    mut components: ErasedComponents,
    paths: Vec<ActorPath>,
    node_id: NodeID,
    pool_info: PoolInfo,
    channel_kind: ChannelKind,
    key_builder: Option<KeyBuilder<T>>,
) -> ChannelStrategy<T> {
    match channel_kind {
        ChannelKind::Forward => {
            assert!((components.len() == 1) || (components.len() > node_id.id as usize));
            if components.len() > 1 {
                // Use NodeID as Index
                let target_node = components
                    .remove(node_id.id as usize)
                    .downcast::<Arc<dyn AbstractComponent<Message = ArconMessage<T>>>>()
                    .unwrap();
                let actor_ref = target_node.actor_ref().hold().expect("failed to fetch");
                ChannelStrategy::Forward(Forward::new(
                    Channel::Local(actor_ref),
                    node_id,
                    pool_info,
                ))
            } else {
                let target_node = components
                    .remove(0)
                    .downcast::<Arc<dyn AbstractComponent<Message = ArconMessage<T>>>>()
                    .unwrap();
                let actor_ref = target_node.actor_ref().hold().expect("failed to fetch");
                ChannelStrategy::Forward(Forward::new(
                    Channel::Local(actor_ref),
                    node_id,
                    pool_info,
                ))
            }
        }
        ChannelKind::Keyed => {
            let mut channels = Vec::new();
            for component in components {
                let target_node = component
                    .downcast::<Arc<dyn AbstractComponent<Message = ArconMessage<T>>>>()
                    .unwrap();
                let actor_ref = target_node.actor_ref().hold().expect("failed to fetch");
                let channel = Channel::Local(actor_ref);
                channels.push(channel);
            }
            for path in paths {
                channels.push(Channel::Remote(path, FlightSerde::Reliable));
            }
            ChannelStrategy::Keyed(Keyed::new(
                channels,
                node_id,
                pool_info,
                key_builder.expect("Keyed ChannelStrategy must have KeyBuilder"),
            ))
        }
        ChannelKind::Console => ChannelStrategy::Console,
        ChannelKind::Mute => ChannelStrategy::Mute,
        _ if components.len() + paths.len() == 0 => ChannelStrategy::Mute,
        _ => todo!("Unimplemented ChannelKind {:?}", channel_kind),
    }
}

pub(crate) trait TypedNodeFactory<T: ArconType>: NodeFactory {
    fn set_key_builder(&mut self, key_builder: KeyBuilder<T>);
    fn set_channel_kind(&mut self, channel_kind: ChannelKind);
    fn untype(self: Rc<Self>) -> Rc<dyn NodeFactory>;
}

impl<OP: Operator<OUT = T>, B: Backend, T: ArconType> TypedNodeFactory<T>
    for NodeConstructor<OP, B>
{
    fn set_key_builder(&mut self, key_builder: KeyBuilder<T>) {
        self.out_key_builder = Some(key_builder);
        self.channel_kind = ChannelKind::Keyed;
    }
    fn set_channel_kind(&mut self, channel_kind: ChannelKind) {
        self.channel_kind = channel_kind;
    }
    fn untype(self: Rc<Self>) -> Rc<dyn NodeFactory> {
        self
    }
}

pub(crate) trait TypedSourceFactory<T: ArconType>: SourceFactory {
    fn set_key_builder(&mut self, key_builder: KeyBuilder<T>);
    fn set_channel_kind(&mut self, channel_kind: ChannelKind);
    fn untype(self: Rc<Self>) -> Rc<dyn SourceFactory>;
}

impl<S: Source<Item = T>, B: Backend, T: ArconType> TypedSourceFactory<T>
    for SourceConstructor<S, B>
{
    fn set_key_builder(&mut self, key_builder: KeyBuilder<T>) {
        self.key_builder = Some(key_builder);
        self.channel_kind = ChannelKind::Keyed;
    }
    fn set_channel_kind(&mut self, channel_kind: ChannelKind) {
        self.channel_kind = channel_kind;
    }
    fn untype(self: Rc<Self>) -> Rc<dyn SourceFactory> {
        self
    }
}

pub trait NodeFactory {
    fn build_nodes(
        &self,
        node_ids: Vec<GlobalNodeId>,
        in_channels: Vec<NodeID>,
        components: ErasedComponents,
        paths: Vec<ActorPath>,
        application: &mut Application,
    ) -> Vec<(GlobalNodeId, ErasedComponent)>;
}

pub trait SourceFactory {
    fn build_source(
        &self,
        components: ErasedComponents,
        paths: Vec<ActorPath>,
        application: &mut Application,
    ) -> ErasedSourceManager;
}

impl<OP: Operator + 'static, B: Backend> NodeFactory for NodeConstructor<OP, B> {
    fn build_nodes(
        &self,
        node_ids: Vec<GlobalNodeId>,
        in_channels: Vec<NodeID>,
        mut components: ErasedComponents,
        paths: Vec<ActorPath>,
        app: &mut Application,
    ) -> Vec<(GlobalNodeId, ErasedComponent)> {
        // Initialize state and manager
        let state_dir = app.arcon_conf().state_dir();
        self.init_state_dir(&state_dir);

        let node_manager = self.create_node_manager(app, &in_channels);

        if components.is_empty() && paths.is_empty() && app.debug_node_enabled() {
            components.push(self.create_debug_node(app));
        }
        for node_id in node_ids {
            // Create the Nodes arguments
            let node_descriptor = format!("{}_{}", self.descriptor, node_id.node_id.id);
            let backend = self.create_backend(node_descriptor.clone(), state_dir.clone());
            let channel_strategy = channel_strategy(
                components.clone(),
                paths.clone(),
                node_id.node_id,
                app.get_pool_info().clone(),
                self.channel_kind,
                self.out_key_builder.clone(),
            );

            // Build the Node
            let node = Node::new(
                node_descriptor,
                channel_strategy,
                self.builder.operator.clone()(),
                self.builder.state.clone()(backend.clone()),
                NodeState::new(node_id.node_id, in_channels.clone(), backend.clone()),
                backend.clone(),
                app.arcon_logger.clone(),
                app.epoch_manager().clone(),
                #[cfg(all(feature = "hardware_counters", target_os = "linux", not(test)))]
                self.builder.conf.perf_events.clone(),
                node_id,
                self.in_key_builder.clone(),
            );
            // Create the node and connect it to the NodeManager
            self.create_node_component(app, node, &node_manager);
        }
        // Start NodeManager
        app.ctrl_system()
            .start_notify(&node_manager)
            .wait_timeout(std::time::Duration::from_millis(2000))
            .expect("Failed to start NodeManager");

        // Fetch all created Nodes on this NodeManager and return them as Erased
        // for the next stage..
        node_manager.on_definition(|cd| {
            cd.nodes
                .iter()
                .map(|(id, (comp, _))| (*id, Arc::new(comp.clone()) as ErasedComponent))
                .collect()
        })
    }
}

#[derive(Clone)]
pub(crate) struct NodeConstructor<OP: Operator + 'static, B: Backend> {
    descriptor: String,
    channel_kind: ChannelKind,
    builder: Arc<OperatorBuilder<OP, B>>,
    in_key_builder: Option<KeyBuilder<OP::IN>>,
    out_key_builder: Option<KeyBuilder<OP::OUT>>,
}

impl<OP: Operator + 'static, B: Backend> NodeConstructor<OP, B> {
    pub fn new(
        descriptor: String,
        builder: Arc<OperatorBuilder<OP, B>>,
        in_key_builder: Option<KeyBuilder<OP::IN>>,
    ) -> NodeConstructor<OP, B> {
        NodeConstructor {
            descriptor,
            channel_kind: ChannelKind::default(),
            builder,
            in_key_builder,
            out_key_builder: None,
        }
    }

    fn create_node_component(
        &self,
        application: &mut Application,
        node: Node<OP, B>,
        node_manager: &Arc<Component<NodeManager<OP, B>>>,
    ) {
        let node_id = node.node_id;
        let node_comp = application.data_system().create(|| node);
        let required_ref: RequiredRef<NodeManagerPort> = node_comp.required_ref();

        biconnect_components::<NodeManagerPort, _, _>(node_manager, &node_comp).expect("fail");

        let node_comp: Arc<dyn AbstractComponent<Message = ArconMessage<OP::IN>>> = node_comp;

        application
            .data_system()
            .start_notify(&node_comp)
            .wait_timeout(std::time::Duration::from_millis(5000))
            .expect("Failed to start Node Component");

        node_manager.on_definition(|cd| {
            // Insert the created Node into the NodeManager
            cd.nodes.insert(node_id, (node_comp.clone(), required_ref));
        });
    }

    fn create_debug_node(&self, application: &mut Application) -> ErasedComponent {
        let debug_node = DebugNode::<OP::OUT>::new();
        let debug_component = application.data_system().create(|| debug_node);
        application
            .data_system()
            .start_notify(&debug_component)
            .wait_timeout(std::time::Duration::from_millis(5000))
            .expect("Failed to start Node Component");

        application.debug_node = Some(debug_component.clone());
        let debug_component: Arc<dyn AbstractComponent<Message = ArconMessage<OP::OUT>>> =
            debug_component;
        let erased = Arc::new(debug_component) as ErasedComponent;

        application.abstract_debug_node = Some(erased.clone());
        erased
    }

    fn create_backend(&self, node_descriptor: String, mut state_dir: PathBuf) -> Arc<B> {
        state_dir.push(&node_descriptor);
        self.builder
            .create_backend(state_dir, node_descriptor.clone())
    }

    /// Initializes things like state directory and NodeManager
    fn create_node_manager(
        &self,
        application: &mut Application,
        in_channels: &[NodeID],
    ) -> Arc<Component<NodeManager<OP, B>>> {
        // Define the NodeManager
        let manager = NodeManager::<OP, B>::new(
            self.descriptor.clone(),
            application.data_system().clone(),
            in_channels.to_vec(),
            application.arcon_logger.clone(),
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
        manager_comp
    }

    fn init_state_dir(&self, path: &PathBuf) {
        // Ensure there's a state_directory
        std::fs::create_dir_all(path).unwrap();
    }
}

#[derive(Clone)]
pub(crate) struct SourceConstructor<S: Source + 'static, B: Backend> {
    descriptor: String,
    builder_type: SourceBuilderType<S, B>,
    time: ArconTime,
    channel_kind: ChannelKind,
    key_builder: Option<KeyBuilder<S::Item>>,
}

impl<S: Source + 'static, B: Backend> SourceConstructor<S, B> {
    pub(crate) fn new(
        descriptor: String,
        builder_type: SourceBuilderType<S, B>,
        time: ArconTime,
    ) -> Self {
        SourceConstructor {
            descriptor,
            builder_type,
            time,
            channel_kind: ChannelKind::default(),
            key_builder: None,
        }
    }

    // Source Manager needs to be (re-)inserted after calling this function
    fn create_source_manager(
        &self,
        app: &mut Application,
        backend: Arc<B>,
    ) -> Arc<Component<SourceManager<B>>> {
        let watermark_interval = app.arcon_conf().watermark_interval;
        let manager = SourceManager::new(
            self.descriptor.clone(),
            self.time,
            watermark_interval,
            app.epoch_manager(),
            backend,
            app.arcon_logger.clone(),
        );
        app.ctrl_system().create(|| manager)
    }

    fn start_source_manager(
        &self,
        source_manager: &Arc<Component<SourceManager<B>>>,
        application: &mut Application,
    ) {
        let source_ref: ActorRefStrong<SourceEvent> =
            source_manager.actor_ref().hold().expect("fail");
        // Set source reference at the EpochManager
        if let Some(epoch_manager) = &application.epoch_manager {
            epoch_manager.on_definition(|cd| {
                cd.source_manager = Some(source_ref);
            });
        }

        application
            .ctrl_system()
            .start_notify(source_manager)
            .wait_timeout(std::time::Duration::from_millis(2000))
            .expect("Failed to start SourceManager");
    }
}

impl<S: Source + 'static, B: Backend> SourceFactory for SourceConstructor<S, B> {
    fn build_source(
        &self,
        components: ErasedComponents,
        paths: Vec<ActorPath>,
        app: &mut Application,
    ) -> ErasedSourceManager {
        let mut state_dir = app.arcon_conf().state_dir();
        state_dir.push("source_manager");
        let backend = Arc::new(B::create(&state_dir, String::from("source_manager")).unwrap());

        let source_manager = self.create_source_manager(app, backend.clone());

        match &self.builder_type {
            SourceBuilderType::Single(builder) => {
                let source_cons = builder.constructor.clone();
                let source_conf = builder.conf.clone();
                let source_index = 0;
                let source = source_cons(backend.clone());
                let channel_strategy = channel_strategy(
                    components.clone(),
                    paths,
                    NodeID::new(source_index as u32),
                    app.get_pool_info(),
                    self.channel_kind,
                    self.key_builder.clone(),
                );
                create_source_node(
                    app,
                    channel_strategy,
                    source_index,
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
                    let source = source_cons(backend.clone(), source_index, parallelism);
                    let channel_strategy = channel_strategy(
                        components.clone(),
                        paths.clone(),
                        NodeID::new(source_index as u32),
                        app.get_pool_info(),
                        self.channel_kind,
                        self.key_builder.clone(),
                    );
                    create_source_node(
                        app,
                        channel_strategy,
                        source_index,
                        source,
                        source_conf,
                        &source_manager,
                    );
                }
            }
        }
        self.start_source_manager(&source_manager, app);
        source_manager
    }
}

// helper function to create source node..
fn create_source_node<S, B>(
    app: &mut Application,
    channel_strategy: ChannelStrategy<S::Item>,
    source_index: usize,
    source: S,
    source_conf: SourceConf<S::Item>,
    source_manager_comp: &Arc<Component<SourceManager<B>>>,
) where
    S: Source,
    B: Backend,
{
    let source_node = SourceNode::new(
        source_index,
        source,
        source_conf,
        channel_strategy,
        app.arcon_logger.clone(),
    );
    let source_node_comp = app.data_system().create(|| source_node);

    biconnect_components::<SourceManagerPort, _, _>(source_manager_comp, &source_node_comp)
        .expect("failed to biconnect components");

    app.data_system()
        .start_notify(&source_node_comp)
        .wait_timeout(std::time::Duration::from_millis(5000))
        .expect("Failed to start Source Node");

    let source_node_comp_dyn: Arc<dyn AbstractComponent<Message = SourceEvent>> = source_node_comp;

    source_manager_comp.on_definition(|cd| {
        cd.add_source(source_node_comp_dyn);
    });
}
