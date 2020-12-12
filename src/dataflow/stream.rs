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
use kompact::{
    component::AbstractComponent,
    prelude::{ActorRefFactory, KompactSystem},
};
use std::{marker::PhantomData, path::Path, sync::Arc};

// Defines a Default State Backend for high-level operators that do not use any
// custom-defined state but still need a backend defined for internal runtime state.
cfg_if::cfg_if! {
    if #[cfg(feature = "rocksdb")]  {
        pub type DefaultBackend = arcon_state::Rocks;
    } else {
        pub type DefaultBackend = arcon_state::Sled;
    }
}

// Helper trait to reduce code in the high-level operator methods
pub trait StreamFnBounds: Send + Sync + Clone + 'static {}
impl<T> StreamFnBounds for T where T: Send + Sync + Clone + 'static {}

#[derive(Default)]
pub struct Context {
    pub(crate) dfg: DFG,
    pipeline: Pipeline,
    source_complete: bool,
    console_output: bool,
}

impl Context {
    pub fn new(pipeline: Pipeline) -> Self {
        Self {
            dfg: Default::default(),
            pipeline,
            source_complete: false,
            console_output: false,
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
        F: Fn(IN) -> OUT + StreamFnBounds,
    {
        self.operator(move |_: Arc<DefaultBackend>| Map::new(f.clone()), |_| ())
    }

    /// Adds a stateful Map transformation to the dataflow graph
    pub fn map_with_state<OUT, S, B, SC, F, C>(self, f: F, sc: SC, conf: C) -> Stream<OUT>
    where
        OUT: ArconType,
        S: ArconState,
        B: Backend,
        SC: Fn(Arc<B>) -> S + 'static,
        F: Fn(IN, &mut S) -> OperatorResult<OUT> + StreamFnBounds,
        C: FnOnce(&mut OperatorConfig),
    {
        self.operator(move |b: Arc<B>| Map::stateful(sc(b), f.clone()), conf)
    }

    /// Adds an in-place Map transformation to the dataflow graph
    pub fn map_in_place<F>(self, f: F) -> Stream<IN>
    where
        F: Fn(&mut IN) + StreamFnBounds,
    {
        self.operator(
            move |_: Arc<DefaultBackend>| MapInPlace::new(f.clone()),
            |_| (),
        )
    }

    /// Adds a stateful in-place Map transformation to the dataflow graph
    pub fn map_in_place_with_state<S, SC, B, F, C>(self, f: F, sc: SC, conf: C) -> Stream<IN>
    where
        S: ArconState,
        B: Backend,
        SC: Fn(Arc<B>) -> S + 'static,
        F: Fn(&mut IN, &mut S) -> OperatorResult<()> + StreamFnBounds,
        C: FnOnce(&mut OperatorConfig),
    {
        self.operator(
            move |b: Arc<B>| MapInPlace::stateful(sc(b), f.clone()),
            conf,
        )
    }

    /// Adds a Filter transformation to the dataflow graph
    pub fn filter<F>(self, f: F) -> Stream<IN>
    where
        F: Fn(&IN) -> bool + StreamFnBounds,
    {
        self.operator(move |_: Arc<DefaultBackend>| Filter::new(f.clone()), |_| ())
    }

    /// Adds a stateful Filter transformation to the dataflow graph
    pub fn filter_with_state<S, SC, B, F, C>(self, f: F, sc: SC, conf: C) -> Stream<IN>
    where
        S: ArconState,
        B: Backend,
        SC: Fn(Arc<B>) -> S + 'static,
        F: Fn(&IN, &mut S) -> bool + StreamFnBounds,
        C: FnOnce(&mut OperatorConfig),
    {
        self.operator(move |b: Arc<B>| Filter::stateful(sc(b), f.clone()), conf)
    }

    /// Adds a FlatMap transformation to the dataflow graph
    pub fn flatmap<OUTS, F>(self, f: F) -> Stream<OUTS::Item>
    where
        OUTS: IntoIterator + 'static,
        OUTS::Item: ArconType,
        F: Fn(IN) -> OUTS + StreamFnBounds,
    {
        self.operator(
            move |_: Arc<DefaultBackend>| FlatMap::new(f.clone()),
            |_| {},
        )
    }

    /// Adds a stateful FlatMap transformation to the dataflow graph
    pub fn flatmap_with_state<OUTS, S, B, SC, F, C>(
        self,
        f: F,
        sc: SC,
        conf: C,
    ) -> Stream<OUTS::Item>
    where
        OUTS: IntoIterator + 'static,
        OUTS::Item: ArconType,
        S: ArconState,
        B: Backend,
        SC: Fn(Arc<B>) -> S + 'static,
        F: Fn(IN, &mut S) -> OperatorResult<OUTS> + StreamFnBounds,
        C: FnOnce(&mut OperatorConfig),
    {
        self.operator(move |b: Arc<B>| FlatMap::stateful(sc(b), f.clone()), conf)
    }

    /// Will make sure the most downstream Node will print its result to the console
    pub fn to_console(mut self) -> Stream<IN> {
        self.ctx.console_output = true;

        Stream {
            _marker: PhantomData,
            prev_dfg_id: self.prev_dfg_id,
            ctx: self.ctx,
        }
    }

    /// This method may be used to add a custom defined [`Operator`] to the dataflow graph
    pub fn operator<OP, B, F, C>(mut self, operator: F, c: C) -> Stream<OP::OUT>
    where
        OP: Operator + 'static,
        B: Backend,
        F: Fn(Arc<B>) -> OP + 'static,
        C: FnOnce(&mut OperatorConfig),
    {
        // Set up config and run the conf closure on it.
        let mut conf = OperatorConfig::default();
        c(&mut conf);

        // Yeah, fix this..
        let is_source =
            self.ctx.dfg.get(&self.prev_dfg_id).is_source() && !self.ctx.source_complete;
        let state_id = conf.state_id.clone();
        let mut state_dir = self.ctx.pipeline.arcon_conf().state_dir.clone();
        let pool_info = self.ctx.pipeline.get_pool_info();

        if is_source {
            // NOTE: Hardcoded Collection Source for now...

            let watermark_interval = self.ctx.pipeline.arcon_conf().watermark_interval;

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
                    state_dir.push(state_id.clone());
                    // set up the backend
                    let backend = Arc::new(B::create(&state_dir).unwrap());
                    let source_ctx = SourceContext::new(
                        watermark_interval,
                        None,
                        channel_strategy,
                        operator(backend.clone()),
                        backend,
                    );
                    let collection_source = CollectionSource::new(collection, source_ctx);
                    let comp = system.create_erased(Box::new(collection_source));
                    system
                        .start_notify(&comp)
                        .wait_timeout(std::time::Duration::from_millis(2000))
                        .expect("");
                    return comp;
                },
            );
            let mut dfg_source = self.ctx.dfg.get_mut(&self.prev_dfg_id);
            dfg_source.config = conf.clone();
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
            // create an entry for this state_id within state_dir directory
            //let mut dir = self.ctx.pipeline.arcon_conf().state_dir.clone();
            state_dir.push(conf.state_id.clone());
            // set up the backend
            let backend = Arc::new(B::create(&state_dir).unwrap());

            let manager_constructor = Box::new(
                |descriptor: String, in_channels: Vec<NodeID>, system: &mut KompactSystem| {
                    let manager = NodeManager::new(descriptor, in_channels, backend.clone());
                    let comp = system.create_erased(Box::new(manager));
                    comp
                },
            );

            let node_constructor: NodeConstructor = Box::new(
                move |descriptor: String,
                      node_id: NodeID,
                      in_channels: Vec<NodeID>,
                      mut components: Vec<Box<dyn std::any::Any>>,
                      channel_kind: ChannelKind,
                      system: &mut KompactSystem| {
                    let channel_strategy = match channel_kind {
                        ChannelKind::Forward => {
                            assert_eq!(components.len(), 1, "Expected a single component target");
                            let component = components.remove(0);
                            let target_node = component
                        .downcast::<Arc<dyn AbstractComponent<Message = ArconMessage<OP::OUT>>>>()
                        .unwrap();
                            let actor_ref =
                                target_node.actor_ref().hold().expect("failed to fetch");
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
                        NodeState::new(node_id, in_channels, backend),
                    );

                    let node_comp = system.create_erased(Box::new(node));

                    system
                        .start_notify(&node_comp)
                        .wait_timeout(std::time::Duration::from_millis(2000))
                        .expect("");

                    Box::new(node_comp) as Box<dyn std::any::Any>
                },
            );

            let next_dfg_id = self.ctx.dfg.insert(DFGNode::new(
                DFGNodeKind::Node(node_constructor),
                conf,
                vec![self.prev_dfg_id],
            ));

            self.prev_dfg_id = next_dfg_id;
        }
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

        let mut target_nodes: Option<Vec<Box<dyn std::any::Any>>> = None;

        for dfg_node in self.ctx.dfg.graph.into_iter().rev() {
            match dfg_node.kind {
                DFGNodeKind::Source(source_kind, channel_kind) => match source_kind {
                    SourceKind::Collection(c) => {
                        let collection: Box<dyn std::any::Any> = c.collection;
                        let constructor = c.constructor.unwrap();
                        let comp = constructor(
                            collection,
                            target_nodes.take().unwrap(),
                            channel_kind,
                            &mut self.ctx.pipeline.system(),
                        );
                        self.ctx.pipeline.add_source_comp(comp);
                    }
                },
                DFGNodeKind::Node(node_cons) => {
                    let (channel_kind, components) = {
                        match target_nodes {
                            Some(comps) => (dfg_node.channel_kind, comps),
                            None => (
                                if self.ctx.console_output {
                                    ChannelKind::Console
                                } else {
                                    ChannelKind::Mute
                                },
                                vec![],
                            ),
                        }
                    };

                    let node: Box<dyn std::any::Any> = node_cons(
                        String::from("node_1"), // Fix
                        NodeID::new(0),         // Fix
                        vec![NodeID::new(0)],   // Fix
                        components,
                        channel_kind,
                        self.ctx.pipeline.system(),
                    );

                    target_nodes = Some(vec![node]);
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
