use crate::{
    buffer::event::PoolInfo,
    data::{ArconMessage, ArconType, NodeID, StateID},
    dataflow::{
        conf::{OperatorBuilder, ParallelismStrategy},
        dfg::ChannelKind,
    },
    manager::{node::NodeManager, source::SourceManager},
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
    prelude::{biconnect_ports, ActorRefFactory, ActorRefStrong, KompactSystem},
};
use std::{any::Any, sync::Arc};

pub type SourceManagerConstructor =
    Box<dyn FnOnce(Vec<SourceComponent>, &mut Pipeline) -> ErasedSourceManager>;
pub type SourceConstructor = Box<
    dyn FnOnce(
        Vec<Arc<dyn Any + Send + Sync>>,
        ChannelKind,
        &mut KompactSystem,
    ) -> Arc<dyn AbstractComponent<Message = SourceEvent>>,
>;
pub type SourceComponent = Arc<dyn AbstractComponent<Message = SourceEvent>>;
pub type ErasedSourceManager = Arc<dyn AbstractComponent<Message = SourceEvent>>;
pub type NodeManagerConstructor =
    Box<dyn FnOnce(Vec<NodeID>, ErasedComponents, ChannelKind, &mut Pipeline) -> ErasedComponents>;

pub type ErasedComponent = Arc<dyn Any + Send + Sync>;
pub type ErasedComponents = Vec<ErasedComponent>;

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

fn channel_strategy<OUT: ArconType>(
    mut components: ErasedComponents,
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
            ChannelStrategy::Forward(Forward::new(Channel::Local(actor_ref), node_id, pool_info))
        }
        ChannelKind::Keyed => {
            let max_key = 256; // fix
            let mut channels = Vec::new();
            for component in components {
                let target_node = component
                    .downcast::<Arc<dyn AbstractComponent<Message = ArconMessage<OUT>>>>()
                    .unwrap();
                let actor_ref = target_node.actor_ref().hold().expect("failed to fetch");
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
pub(crate) fn node_manager_constructor<OP: Operator + 'static, B: Backend>(
    descriptor: String,
    data_system: KompactSystem,
    builder: OperatorBuilder<OP, B>,
    backend: Arc<B>,
) -> NodeManagerConstructor {
    Box::new(
        move |in_channels: Vec<NodeID>,
              components: ErasedComponents,
              channel_kind: ChannelKind,
              pipeline: &mut Pipeline| {
            let epoch_manager_ref = pipeline.epoch_manager();

            // Define the NodeManager
            let manager = NodeManager::<OP, B>::new(
                descriptor.clone(),
                data_system,
                epoch_manager_ref,
                in_channels.clone(),
                backend.clone(),
            );

            // Create the actual NodeManager component
            let manager_comp = pipeline.ctrl_system().create(|| manager);

            // Connect NodeManager to the SnapshotManager of the pipeline
            pipeline.snapshot_manager.on_definition(|scd| {
                manager_comp.on_definition(|cd| {
                    biconnect_ports(&mut scd.manager_port, &mut cd.snapshot_manager_port);
                });
            });

            // Start NodeManager
            pipeline
                .ctrl_system()
                .start_notify(&manager_comp)
                .wait_timeout(std::time::Duration::from_millis(2000))
                .expect("Failed to start NodeManager");

            let instances = match builder.conf.parallelism_strategy {
                ParallelismStrategy::Static(s) => s,
                _ => panic!("Not supported yet"),
            };

            let mut curr_node_id = 0;
            let pool_info = pipeline.get_pool_info();
            let operator = builder.constructor;

            for _ in 0..instances {
                let node_descriptor = format!("{}_{}", descriptor, curr_node_id);
                let node_id = NodeID::new(curr_node_id);

                let node = Node::new(
                    node_descriptor,
                    channel_strategy(components.clone(), node_id, pool_info.clone(), channel_kind),
                    operator(backend.clone()),
                    NodeState::new(node_id, in_channels.clone(), backend.clone()),
                    backend.clone(),
                );

                let node_comp = pipeline.data_system().create(|| node);
                let required_ref = node_comp.on_definition(|cd| cd.node_manager_port.share());
                let node_comp: Arc<dyn AbstractComponent<Message = ArconMessage<OP::IN>>> =
                    node_comp;

                pipeline
                    .data_system()
                    .start_notify(&node_comp)
                    .wait_timeout(std::time::Duration::from_millis(2000))
                    .expect("Failed to start Node Component");

                manager_comp.on_definition(|cd| {
                    // Connect NodeManagerPort
                    node_comp.on_dyn_definition(|ncd| {
                        let required_port = ncd.get_required_port().unwrap();
                        biconnect_ports(&mut cd.manager_port, required_port);
                    });

                    // Insert the created Node into the NodeManager
                    cd.nodes.insert(node_id, (node_comp, required_ref));
                });

                curr_node_id += 1;
            }

            let nodes: ErasedComponents = manager_comp.on_definition(|cd| {
                cd.nodes
                    .values()
                    .map(|(comp, _)| Arc::new(comp.clone()) as ErasedComponent)
                    .collect()
            });

            nodes
        },
    )
}
