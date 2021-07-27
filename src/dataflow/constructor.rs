// Copyright (c) 2021, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    application::conf::logger::ArconLogger,
    application::Application,
    buffer::event::PoolInfo,
    data::{ArconMessage, ArconType, NodeID},
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
use std::{any::Any, convert::TryInto, sync::Arc};

pub type SourceManagerConstructor = Box<
    dyn FnOnce(
        Vec<Arc<dyn Any + Send + Sync>>,
        ChannelKind,
        &mut Application,
    ) -> ErasedSourceManager,
>;
pub type SourceConstructor = Box<
    dyn FnOnce(
        Vec<Arc<dyn Any + Send + Sync>>,
        ChannelKind,
        &mut KompactSystem,
    ) -> Arc<dyn AbstractComponent<Message = SourceEvent>>,
>;
pub type ErasedSourceManager = Arc<dyn AbstractComponent<Message = SourceEvent>>;
pub type NodeManagerConstructor = Box<
    dyn FnOnce(Vec<NodeID>, ErasedComponents, ChannelKind, &mut Application) -> ErasedComponents,
>;

pub type ErasedComponent = Arc<dyn Any + Send + Sync>;
pub type ErasedComponents = Vec<ErasedComponent>;

fn channel_strategy<OUT: ArconType>(
    mut components: ErasedComponents,
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
            ChannelStrategy::Keyed(Keyed::new(max_key, channels, node_id, pool_info))
        }
        ChannelKind::Console => ChannelStrategy::Console,
        ChannelKind::Mute => ChannelStrategy::Mute,
        _ => unimplemented!(),
    }
}

pub(crate) fn source_manager_constructor<S: Source + 'static, B: Backend>(
    descriptor: String,
    builder_type: SourceBuilderType<S, B>,
    backend: Arc<B>,
    watermark_interval: u64,
    time: ArconTime,
) -> SourceManagerConstructor {
    Box::new(
        move |components: Vec<Arc<dyn std::any::Any + Send + Sync>>,
              channel_kind: ChannelKind,
              app: &mut Application| {
            let epoch_manager_ref = app.epoch_manager();

            let manager = SourceManager::new(
                descriptor,
                time,
                watermark_interval,
                epoch_manager_ref,
                backend.clone(),
                app.arcon_logger.clone(),
            );
            let source_manager_comp = app.ctrl_system().create(|| manager);

            match builder_type {
                SourceBuilderType::Single(builder) => {
                    let source_cons = builder.constructor;
                    let source_conf = builder.conf;
                    let source_index = 0;
                    let source = source_cons(backend.clone());
                    create_source_node(
                        app,
                        source_index,
                        components.clone(),
                        channel_kind,
                        source,
                        source_conf,
                        &source_manager_comp,
                    );
                }
                SourceBuilderType::Parallel(builder) => {
                    let source_cons = builder.constructor;
                    let parallelism = builder.parallelism;
                    for source_index in 0..builder.parallelism {
                        let source_conf = builder.conf.clone();
                        let source = source_cons(backend.clone(), source_index, parallelism); // todo
                        create_source_node(
                            app,
                            source_index,
                            components.clone(),
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
            if let Some(epoch_manager) = &app.epoch_manager {
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
    )
}

// helper function to create source node..
fn create_source_node<S, B>(
    app: &mut Application,
    source_index: usize,
    components: Vec<Arc<dyn std::any::Any + Send + Sync>>,
    channel_kind: ChannelKind,
    source: S,
    source_conf: SourceConf<S::Item>,
    source_manager_comp: &Arc<Component<SourceManager<B>>>,
) where
    S: Source,
    B: Backend,
{
    let pool_info = app.get_pool_info();
    let max_key = app.conf.max_key;
    let channel_strategy = channel_strategy(
        components.clone(),
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
        app.arcon_logger.clone(),
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

pub(crate) fn node_manager_constructor<OP: Operator + 'static, B: Backend>(
    descriptor: String,
    data_system: KompactSystem,
    builder: OperatorBuilder<OP, B>,
    backend: Arc<B>,
    logger: ArconLogger,
) -> NodeManagerConstructor {
    Box::new(
        move |in_channels: Vec<NodeID>,
              components: ErasedComponents,
              channel_kind: ChannelKind,
              app: &mut Application| {
            let epoch_manager_ref = app.epoch_manager();

            // How many instances of this Operator we are initially creating
            let instances = match builder.conf.parallelism_strategy {
                ParallelismStrategy::Static(s) => s,
                _ => panic!("Managed ParallelismStrategy not supported yet"),
            };

            let max_key = app.conf.max_key as usize;

            // Define the NodeManager
            let manager = NodeManager::<OP, B>::new(
                descriptor.clone(),
                data_system,
                epoch_manager_ref,
                in_channels.clone(),
                backend.clone(),
                logger.clone(),
            );

            // Create the actual NodeManager component
            let manager_comp = app.ctrl_system().create(|| manager);

            // Connect NodeManager to the SnapshotManager of the app
            app.snapshot_manager.on_definition(|scd| {
                manager_comp.on_definition(|cd| {
                    biconnect_ports(&mut scd.manager_port, &mut cd.snapshot_manager_port);
                });
            });

            app.query_manager.on_definition(|scd| {
                manager_comp.on_definition(|cd| {
                    biconnect_ports(&mut scd.manager_port, &mut cd.query_manager_port);
                });
            });

            // Start NodeManager
            app.ctrl_system()
                .start_notify(&manager_comp)
                .wait_timeout(std::time::Duration::from_millis(2000))
                .expect("Failed to start NodeManager");

            // Fetch PoolInfo object that ChannelStrategies use to organise their buffers
            let pool_info = app.get_pool_info();
            // Fetch the Operator constructor from the builder
            let operator = builder.constructor;

            // Create `instances` number of Nodes and add them into the NodeManager
            for (curr_node_id, _) in (0..instances).enumerate() {
                let node_descriptor = format!("{}_{}", descriptor, curr_node_id);
                let node_id = NodeID::new(curr_node_id.try_into().unwrap());

                let node = Node::new(
                    node_descriptor,
                    channel_strategy(
                        components.clone(),
                        node_id,
                        pool_info.clone(),
                        max_key as u64,
                        channel_kind,
                    ),
                    operator(backend.clone()),
                    NodeState::new(node_id, in_channels.clone(), backend.clone()),
                    backend.clone(),
                    app.arcon_logger.clone(),
                    #[cfg(all(feature = "hardware_counters", target_os = "linux", not(test)))]
                    builder.conf.perf_events.clone(),
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
    )
}
