use crate::{
    buffer::event::PoolInfo,
    data::{NodeID, StateID},
    dataflow::dfg::ChannelKind,
    manager::{
        node::{NodeManager, NodeManagerPort},
        source::SourceManager,
    },
    pipeline::Pipeline,
    prelude::ArconMessage,
    stream::{
        channel::{
            strategy::{forward::Forward, *},
            Channel,
        },
        node::{
            source::{SourceEvent, SourceNode},
            Node, NodeState,
        },
        operator::Operator,
        source::Source,
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
        Vec<Box<dyn Any>>,
        ChannelKind,
        &mut KompactSystem,
    ) -> Arc<dyn AbstractComponent<Message = SourceEvent>>,
>;

pub(crate) fn source_cons<S>(source: S, pool_info: PoolInfo) -> SourceConstructor
where
    S: Source,
{
    Box::new(
        move |mut components: Vec<Box<dyn std::any::Any>>,
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
    Box<dyn FnOnce(StateID, Vec<SourceComponent>, &mut Pipeline) -> ErasedSourceManager>;

pub(crate) fn source_manager_cons<B: Backend>(
    backend: Arc<B>,
    watermark_interval: u64,
) -> SourceManagerConstructor {
    Box::new(
        move |state_id: StateID, source_comps, pipeline: &mut Pipeline| {
            let epoch_manager_ref = pipeline.epoch_manager();

            let manager = SourceManager::new(
                state_id,
                watermark_interval,
                source_comps,
                epoch_manager_ref,
                backend.clone(),
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
        },
    )
}

pub type NodeConstructor = Box<
    dyn FnOnce(
        String,
        NodeID,
        Vec<NodeID>,
        Vec<Box<dyn Any>>,
        ChannelKind,
        &mut KompactSystem,
        ErasedNodeManager,
    ) -> Box<dyn Any>,
>;

pub(crate) fn node_cons<OP, B, F>(
    operator: F,
    backend: Arc<B>,
    pool_info: PoolInfo,
) -> NodeConstructor
where
    OP: Operator + 'static,
    B: Backend,
    F: Fn(Arc<B>) -> OP + 'static,
{
    Box::new(
        move |descriptor: String,
              node_id: NodeID,
              in_channels: Vec<NodeID>,
              mut components: Vec<Box<dyn std::any::Any>>,
              channel_kind: ChannelKind,
              system: &mut KompactSystem,
              manager: ErasedNodeManager| {
            let channel_strategy = match channel_kind {
                ChannelKind::Forward => {
                    assert_eq!(components.len(), 1, "Expected a single component target");
                    let component = components.remove(0);
                    let target_node = component
                        .downcast::<Arc<dyn AbstractComponent<Message = ArconMessage<OP::OUT>>>>()
                        .unwrap();
                    let actor_ref = target_node.actor_ref().hold().expect("failed to fetch");
                    ChannelStrategy::Forward(Forward::new(
                        Channel::Local(actor_ref),
                        node_id,
                        pool_info,
                    ))
                }
                ChannelKind::Console => ChannelStrategy::Console,
                ChannelKind::Mute => ChannelStrategy::Mute,
                _ => unimplemented!(),
            };

            let node = Node::new(
                descriptor,
                channel_strategy,
                operator(backend.clone()),
                NodeState::new(node_id, in_channels, backend.clone()),
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

            Box::new(node_comp) as Box<dyn std::any::Any>
        },
    )
}

pub type NodeManagerConstructor =
    Box<dyn FnOnce(String, Vec<NodeID>, &mut Pipeline) -> ErasedNodeManager>;

pub(crate) fn node_manager_cons<B: Backend>(backend: Arc<B>) -> NodeManagerConstructor {
    Box::new(
        move |descriptor: String, in_channels: Vec<NodeID>, pipeline: &mut Pipeline| {
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
        },
    )
}
