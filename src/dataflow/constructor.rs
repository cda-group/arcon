use crate::{
    buffer::event::PoolInfo,
    data::{ArconMessage, ArconType, NodeID, StateID},
    dataflow::dfg::ChannelKind,
    manager::{
        node::{NodeManager, NodeManagerPort},
        source::SourceManager,
    },
    pipeline::Pipeline,
    stream::{
        channel::{
            strategy::{forward::Forward, key_by::KeyBy, *},
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
    prelude::{biconnect_ports, ActorRefFactory, ActorRefStrong, KompactSystem, Never},
};
use std::{any::Any, sync::Arc};

pub type SourceConstructor = Box<
    dyn FnOnce(
        Vec<Arc<dyn Any + Send + Sync>>,
        ChannelKind,
        &mut KompactSystem,
    ) -> Arc<dyn AbstractComponent<Message = SourceEvent>>,
>;

pub(crate) fn source_cons<S>(source: S, pool_info: PoolInfo) -> SourceConstructor
where
    S: Source,
{
    Box::new(
        move |mut components: Vec<Arc<dyn std::any::Any + Send + Sync>>,
              channel_kind: ChannelKind,
              system: &mut KompactSystem| {
            let channel_strategy = match channel_kind {
                ChannelKind::Forward => {
                    let component = components.remove(0);
                    let target_node = component
                        .downcast::<Arc<dyn AbstractComponent<Message = ArconMessage<S::Data>>>>()
                        .unwrap();
                    let actor_ref = target_node.actor_ref().hold().expect("failed to fetch");
                    ChannelStrategy::Forward(Forward::new(
                        Channel::Local(actor_ref),
                        0.into(),
                        pool_info,
                    ))
                }
                _ => panic!("TODO"),
            };
            let source_node = SourceNode::new(source, channel_strategy);
            let comp = system.create_erased(Box::new(source_node));
            system
                .start_notify(&comp)
                .wait_timeout(std::time::Duration::from_millis(2000))
                .expect("");

            comp
        },
    )
}

pub type SourceComponent = Arc<dyn AbstractComponent<Message = SourceEvent>>;
pub type ErasedSourceManager = Arc<dyn AbstractComponent<Message = SourceEvent>>;
pub type ErasedNodeManager = Arc<dyn AbstractComponent<Message = Never>>;

pub type SourceManagerConstructor =
    Box<dyn FnOnce(Vec<SourceComponent>, &mut Pipeline) -> ErasedSourceManager>;

pub(crate) fn source_manager_cons<B: Backend>(
    state_id: StateID,
    backend: Arc<B>,
    watermark_interval: u64,
    time: ArconTime,
) -> SourceManagerConstructor {
    Box::new(move |source_comps, pipeline: &mut Pipeline| {
        let epoch_manager_ref = pipeline.epoch_manager();

        let manager = SourceManager::new(
            state_id,
            time,
            watermark_interval,
            source_comps,
            epoch_manager_ref,
            backend,
        );
        let comp = pipeline.ctrl_system().create_erased(Box::new(manager));

        let source_ref: ActorRefStrong<SourceEvent> = comp.actor_ref().hold().expect("fail");

        // Set source reference at the EpochManager
        if let Some(epoch_manager) = &pipeline.epoch_manager {
            epoch_manager.on_definition(|cd| {
                cd.source_manager = Some(source_ref);
            });
        }

        pipeline
            .ctrl_system()
            .start_notify(&comp)
            .wait_timeout(std::time::Duration::from_millis(2000))
            .expect("Failed to start SourceManager");

        comp
    })
}

pub type NodeConstructor = Box<
    dyn FnOnce(
        String,
        NodeID,
        Vec<NodeID>,
        Vec<Arc<dyn Any + Send + Sync>>,
        ChannelKind,
        &mut KompactSystem,
        ErasedNodeManager,
    ) -> Vec<Arc<dyn std::any::Any + Send + Sync>>,
>;

pub(crate) fn node_cons<OP, B, F: ?Sized>(
    operator: Arc<F>,
    backend: Arc<B>,
    pool_info: PoolInfo,
) -> NodeConstructor
where
    OP: Operator + 'static,
    B: Backend,
    F: Fn(Arc<B>) -> OP + Send + Sync + 'static,
{
    Box::new(
        move |descriptor: String,
              node_id: NodeID,
              in_channels: Vec<NodeID>,
              components: Vec<Arc<dyn std::any::Any + Send + Sync>>,
              channel_kind: ChannelKind,
              system: &mut KompactSystem,
              manager: ErasedNodeManager| {
            fn channel_strategy<OUT: ArconType>(
                mut components: Vec<Arc<dyn std::any::Any + Send + Sync>>,
                node_id: NodeID,
                pool_info: PoolInfo,
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
                        ChannelStrategy::Forward(Forward::new(
                            Channel::Local(actor_ref),
                            node_id,
                            pool_info,
                        ))
                    }
                    ChannelKind::KeyBy => {
                        let max_key = 256; // fix
                        let mut channels = Vec::new();
                        for component in components {
                            let target_node = component
                        .downcast::<Arc<dyn AbstractComponent<Message = ArconMessage<OUT>>>>()
                        .unwrap();
                            let actor_ref =
                                target_node.actor_ref().hold().expect("failed to fetch");
                            let channel = Channel::Local(actor_ref);
                            channels.push(channel);
                        }
                        ChannelStrategy::KeyBy(KeyBy::new(max_key, channels, node_id, pool_info))
                    }
                    ChannelKind::Console => ChannelStrategy::Console,
                    ChannelKind::Mute => ChannelStrategy::Mute,
                    _ => unimplemented!(),
                }
            }

            let mut nodes = Vec::new();

            let node = Node::new(
                descriptor,
                channel_strategy(components, node_id, pool_info, channel_kind),
                operator(backend.clone()),
                NodeState::new(node_id, in_channels, backend.clone()),
                backend,
            );

            let node_comp = system.create_erased(Box::new(node));

            // Connect node_comp with NodeManager
            manager.on_dyn_definition(|cd| {
                let nm_port = cd.get_provided_port::<NodeManagerPort>().unwrap();
                node_comp.on_dyn_definition(|ncd| {
                    let node_port = ncd.get_required_port::<NodeManagerPort>().unwrap();
                    biconnect_ports(nm_port, node_port);
                });
            });

            system
                .start_notify(&node_comp)
                .wait_timeout(std::time::Duration::from_millis(2000))
                .expect("");

            nodes.push(Arc::new(node_comp) as Arc<dyn Any + Send + Sync>);

            nodes
        },
    )
}

pub type NodeManagerConstructor = Box<dyn FnOnce(Vec<NodeID>, &mut Pipeline) -> ErasedNodeManager>;

pub(crate) fn node_manager_cons<B: Backend>(
    descriptor: String,
    backend: Arc<B>,
) -> NodeManagerConstructor {
    Box::new(move |in_channels: Vec<NodeID>, pipeline: &mut Pipeline| {
        let epoch_manager_ref = pipeline.epoch_manager();
        let manager = NodeManager::new(descriptor, epoch_manager_ref, in_channels, backend);
        let comp = pipeline.ctrl_system().create_erased(Box::new(manager));

        // connect NodeManager to the pipelines SnapshotManager
        pipeline.snapshot_manager.on_definition(|scd| {
            comp.on_dyn_definition(|cd| match cd.get_required_port() {
                Some(p) => biconnect_ports(&mut scd.manager_port, p),
                None => {
                    panic!("Failed to connect NodeManager port to SnapshotManager")
                }
            });
        });

        pipeline
            .ctrl_system()
            .start_notify(&comp)
            .wait_timeout(std::time::Duration::from_millis(2000))
            .expect("Failed to start NodeManager");
        comp
    })
}
