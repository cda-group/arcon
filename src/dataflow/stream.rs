// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconType, NodeID},
    dataflow::dfg::{
        ChannelKind, CollectionConstructor, DFGNode, DFGNodeID, DFGNodeKind, NodeConstructor,
        OperatorConfig, SourceKind, DFG,
    },
    manager::node::NodeManager,
    pipeline::{ErasedNode, Pipeline},
    prelude::{
        ArconMessage, Channel, ChannelStrategy, CollectionSource, Filter, FlatMap, Forward, Map,
        MapInPlace, Node, NodeState,
    },
    stream::{
        operator::Operator,
        source::{ArconSource, SourceContext},
    },
    util::SafelySendableFn,
};
use arcon_error::OperatorResult;
use arcon_state::{index::ArconState, Backend, BackendType};
use downcast::*;
use kompact::{
    component::AbstractComponent,
    prelude::{ActorRefFactory, KompactSystem},
};
use std::{marker::PhantomData, path::Path, sync::Arc};

pub trait ChannelTrait {}
downcast!(dyn ChannelTrait);

#[derive(Default)]
pub struct Context {
    pub(crate) dfg: DFG,
    pipeline: Pipeline,
    source_complete: bool,
}

impl Context {
    pub fn new(pipeline: Pipeline) -> Self {
        Self {
            dfg: Default::default(),
            pipeline,
            source_complete: false,
        }
    }
}

pub struct Stream<IN: ArconType> {
    _marker: PhantomData<IN>,
    /// ID of the node which outputs this stream.
    prev_dfg_id: DFGNodeID,
    ctx: Context,
}

impl<IN: ArconType> Stream<IN> {
    /// Adds a Map transformation to the dataflow graph
    pub fn map<OUT, F>(self, f: F) -> Stream<OUT>
    where
        OUT: ArconType,
        F: Fn(IN) -> OUT + Send + Sync + 'static,
    {
        self.operator(Map::new(f), OperatorConfig::default())
    }

    /// Adds a stateful Map transformation to the dataflow graph
    ///
    /// A state constructor `SC` must be defined and passed in.
    pub fn map_with_state<OUT, S, SC, F, C>(self, f: F, sc: SC, conf: C) -> Stream<OUT>
    where
        OUT: ArconType,
        S: ArconState,
        SC: SafelySendableFn() -> S,
        F: Fn(IN, &mut S) -> OperatorResult<OUT> + Send + Sync + 'static,
        C: FnOnce(&mut OperatorConfig),
    {
        let mut c = OperatorConfig::default();
        conf(&mut c);
        self.operator(Map::stateful(sc(), f), c)
    }

    /// Adds an in-place Map transformation to the dataflow graph
    pub fn map_in_place<F>(self, f: F) -> Stream<IN>
    where
        F: Fn(&mut IN) + Send + Sync + 'static,
    {
        self.operator(MapInPlace::new(f), OperatorConfig::default())
    }

    /// Adds a stateful in-place Map transformation to the dataflow graph
    ///
    /// A state constructor `SC` must be defined and passed in.
    pub fn map_in_place_with_state<S, SC, F, C>(self, f: F, sc: SC, conf: C) -> Stream<IN>
    where
        S: ArconState,
        SC: SafelySendableFn() -> S,
        F: Fn(&mut IN, &mut S) -> OperatorResult<()> + Send + Sync + 'static,
        C: FnOnce(&mut OperatorConfig),
    {
        let mut c = OperatorConfig::default();
        conf(&mut c);
        self.operator(MapInPlace::stateful(sc(), f), c)
    }

    /// Adds a Filter transformation to the dataflow graph
    pub fn filter<F>(self, f: F) -> Stream<IN>
    where
        F: Fn(&IN) -> bool + Send + Sync + 'static,
    {
        self.operator(Filter::new(f), OperatorConfig::default())
    }

    /// Adds a stateful Filter transformation to the dataflow graph
    ///
    /// A state constructor `SC` must be defined and passed in.
    pub fn filter_with_state<S, SC, F, C>(self, f: F, sc: SC, conf: C) -> Stream<IN>
    where
        S: ArconState,
        SC: SafelySendableFn() -> S,
        F: Fn(&IN, &mut S) -> bool + Send + Sync + 'static,
        C: FnOnce(&mut OperatorConfig),
    {
        let mut c = OperatorConfig::default();
        conf(&mut c);
        self.operator(Filter::stateful(sc(), f), c)
    }

    /// Adds a FlatMap transformation to the dataflow graph
    pub fn flatmap<OUTS, F>(self, f: F) -> Stream<OUTS::Item>
    where
        OUTS: IntoIterator + 'static,
        OUTS::Item: ArconType,
        F: Fn(IN) -> OUTS + Send + Sync + 'static,
    {
        self.operator(FlatMap::new(f), OperatorConfig::default())
    }

    /// Adds a stateful FlatMap transformation to the dataflow graph
    ///
    /// A state constructor `SC` must be defined and passed in.
    pub fn flatmap_with_state<OUTS, S, SC, F, C>(self, f: F, sc: SC, conf: C) -> Stream<OUTS::Item>
    where
        OUTS: IntoIterator + 'static,
        OUTS::Item: ArconType,
        S: ArconState,
        SC: SafelySendableFn() -> S,
        F: Fn(IN, &mut S) -> OperatorResult<OUTS> + Send + Sync + 'static,
        C: FnOnce(&mut OperatorConfig),
    {
        let mut c = OperatorConfig::default();
        conf(&mut c);
        self.operator(FlatMap::stateful(sc(), f), c)
    }

    /// Adds a DebugNode as a terminal node
    ///
    /// This method does not return a Stream but rather builds the [`Pipeline`]
    pub fn debug(self) -> Pipeline {
        unimplemented!();
    }

    /// This method may be used to add a custom defined [`Operator`] to the dataflow graph
    pub fn operator<OP: Operator + 'static>(
        mut self,
        operator: OP,
        conf: OperatorConfig,
    ) -> Stream<OP::OUT> {
        // Yeah, fix this..
        let is_source =
            self.ctx.dfg.get(&self.prev_dfg_id).is_source() && !self.ctx.source_complete;
        let backend_type = conf.backend_type;
        let state_id = conf.state_id.clone();
        let pool_info = self.ctx.pipeline.get_pool_info();

        if is_source {
            // NOTE: Hardcoded Collection Source for now...

            let cons: CollectionConstructor = Box::new(
                move |collection: Box<dyn std::any::Any>,
                      mut components: Vec<Box<dyn std::any::Any>>,
                      channel_kind: ChannelKind,
                      system: &mut KompactSystem| {
                    let channel_strategy = match channel_kind {
                        ChannelKind::Forward => {
                            let component = components.remove(0);
                            let target_node = component
                        .downcast::<Arc<dyn AbstractComponent<Message = ArconMessage<OP::OUT>>>>()
                        .unwrap();
                            let actor_ref =
                                target_node.actor_ref().hold().expect("failed to fetch");
                            ChannelStrategy::Forward(Forward::new(
                                Channel::Local(actor_ref),
                                0.into(),
                                pool_info,
                            ))
                        }
                        _ => return unimplemented!(),
                    };

                    let collection: Vec<OP::IN> = *collection.downcast().unwrap();
                    arcon_state::with_backend_type!(backend_type, |SB| {
                        let backend = Arc::new(SB::create(Path::new(&state_id)).unwrap());
                        let source_ctx =
                            SourceContext::new(10, None, channel_strategy, operator, backend);
                        let collection_source = CollectionSource::new(collection, source_ctx);
                        let comp = system.create_erased(Box::new(collection_source));
                        system
                            .start_notify(&comp)
                            .wait_timeout(std::time::Duration::from_millis(2000))
                            .expect("");
                        return comp;
                    });
                },
            );
            let mut dfg_source = self.ctx.dfg.get_mut(&self.prev_dfg_id);
            dfg_source.config = conf;
            let source_kind = match &mut dfg_source.kind {
                DFGNodeKind::Source(s, _) => s,
                _ => panic!("Expected a source kind"),
            };
            match source_kind {
                SourceKind::Collection(col) => {
                    col.constructor = Some(cons);
                }
            }
            self.ctx.source_complete = true;
        } else {
            // Set up constructors for Node and a NodeManager
            arcon_state::with_backend_type!(conf.backend_type, |SB| {
                let backend = Arc::new(SB::create(std::path::Path::new(&conf.state_id)).unwrap());

                let manager_constructor =
                    Box::new(|descriptor: String, in_channels: Vec<NodeID>| {
                        let manager = NodeManager::new(descriptor, in_channels, backend.clone());
                    });

                let node_constructor: NodeConstructor = Box::new(
                    |descriptor: String,
                     node_id: NodeID,
                     in_channels: Vec<NodeID>,
                     mut components: Vec<Box<dyn std::any::Any>>,
                     channel_kind: ChannelKind,
                     system: &mut KompactSystem| {
                        let channel_strategy = match channel_kind {
                            ChannelKind::Forward => {
                                assert_eq!(
                                    components.len(),
                                    1,
                                    "Expected a single component target"
                                );
                                let component = components.remove(0);
                                let target_node = component
                        .downcast::<Arc<dyn AbstractComponent<Message = ArconMessage<OP::OUT>>>>()
                        .unwrap();
                                let actor_ref =
                                    target_node.actor_ref().hold().expect("failed to fetch");
                                ChannelStrategy::Forward(Forward::new(
                                    Channel::Local(actor_ref),
                                    0.into(),
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
                            operator,
                            NodeState::new(node_id, in_channels, backend),
                        );

                        let node_comp = system.create_erased(Box::new(node));

                        system
                            .start_notify(&node_comp)
                            .wait_timeout(std::time::Duration::from_millis(2000))
                            .expect("");

                        Box::new(node_comp) as Box<dyn std::any::Any> // Cast Arc<AbstractComponent<Message = ArconMessage<IN>> to Box<Any>
                    },
                );
                let next_dfg_id = self.ctx.dfg.insert(DFGNode::new(
                    DFGNodeKind::Node(node_constructor),
                    conf,
                    vec![self.prev_dfg_id],
                ));
                self.prev_dfg_id = next_dfg_id;
            });
        }

        // Collection(Filter) -> Map
        //
        // 1. Build Map and return Component only
        // 2. Build Map and return Component + Box<dyn ChannelTrait> for next in line?..
        // For terminal node, we can hardcode "ChannelStrategy::Mute
        //
        // Map(Box<dyn ChannelTrait>, in_channels, node_id,

        Stream {
            _marker: PhantomData,
            prev_dfg_id: self.prev_dfg_id,
            ctx: self.ctx,
        }
    }

    /// Builds the Dataflow graph
    ///
    /// Returns a [`Pipeline`] where all runtime components
    /// have been conneted and started.
    ///
    /// Note that this method only builds the pipeline. In order
    /// to start it, see the following [method](Pipeline::start).
    pub fn build(mut self) -> Pipeline {
        let dfg_len = self.ctx.dfg.len();
        println!("GRAPH LEN {}", dfg_len);

        let mut prev_nodes: Option<Vec<Box<dyn std::any::Any>>> = None;

        for dfg_node in self.ctx.dfg.graph.into_iter().rev() {
            match dfg_node.kind {
                DFGNodeKind::Source(source_kind, channel_kind) => match source_kind {
                    SourceKind::Collection(c) => {
                        let collection: Box<dyn std::any::Any> = c.collection;
                        let constructor = c.constructor.unwrap();
                        let comp = constructor(
                            collection,
                            prev_nodes.take().unwrap(),
                            channel_kind,
                            &mut self.ctx.pipeline.system(),
                        );
                        self.ctx.pipeline.add_source_comp(comp);
                    }
                },
                DFGNodeKind::Node(node_cons) => {
                    let (channel_kind, components) = {
                        match prev_nodes {
                            Some(comps) => (dfg_node.channel_kind, comps),
                            None => (ChannelKind::Console, vec![]),
                        }
                    };

                    let node: Box<dyn std::any::Any> = node_cons(
                        String::from("nodey"),
                        NodeID::new(0),
                        vec![NodeID::new(0)],
                        components,
                        channel_kind,
                        self.ctx.pipeline.system(),
                    );
                    prev_nodes = Some(vec![node]);
                }
            }
        }
        self.ctx.pipeline
    }

    pub(crate) fn new(ctx: Context) -> Self {
        Self {
            _marker: PhantomData,
            prev_dfg_id: DFGNodeID(0),
            ctx,
        }
    }
}
