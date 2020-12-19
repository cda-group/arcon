// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconType, NodeID},
    dataflow::{
        constructor::*,
        dfg::{ChannelKind, DFGNode, DFGNodeID, DFGNodeKind, OperatorConfig, SourceKind, DFG},
    },
    pipeline::{AssembledPipeline, Pipeline},
    prelude::{Filter, FlatMap, Map, MapInPlace},
    stream::operator::Operator,
};
use arcon_error::OperatorResult;
use arcon_state::{index::ArconState, Backend};
use std::{marker::PhantomData, sync::Arc};

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
    console_output: bool,
}

impl Context {
    pub fn new(pipeline: Pipeline) -> Self {
        Self {
            dfg: Default::default(),
            pipeline,
            console_output: false,
        }
    }
}

/// High-level object representing a sequence of stream transformations.
pub struct Stream<IN: ArconType> {
    _marker: PhantomData<IN>,
    // ID of the node which outputs this stream.
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
        let pool_info = self.ctx.pipeline.get_pool_info();

        // Set up directory for the operator and create Backend
        let mut state_dir = self.ctx.pipeline.arcon_conf().state_dir.clone();
        state_dir.push(conf.state_id.clone());
        let backend = Arc::new(B::create(&state_dir).unwrap());

        let node_constructor = node_cons(operator, backend.clone(), pool_info);
        let manager_constructor = node_manager_cons(backend);

        let next_dfg_id = self.ctx.dfg.insert(DFGNode::new(
            DFGNodeKind::Node(node_constructor, manager_constructor),
            conf,
            vec![self.prev_dfg_id],
        ));

        self.prev_dfg_id = next_dfg_id;
        Stream {
            _marker: PhantomData,
            prev_dfg_id: self.prev_dfg_id,
            ctx: self.ctx,
        }
    }

    /// Builds the Dataflow graph
    ///
    /// Returns a [`AssembledPipeline`] where all runtime components
    /// have been conneted and started.
    ///
    /// Note that this method only builds the pipeline. In order
    /// to start it, see the following [method](AssembledPipeline::start).
    pub fn build(mut self) -> AssembledPipeline {
        let mut target_nodes: Option<Vec<Box<dyn std::any::Any>>> = None;

        for dfg_node in self.ctx.dfg.graph.into_iter().rev() {
            match dfg_node.kind {
                DFGNodeKind::Source(source_kind, channel_kind, source_manager_cons) => {
                    match source_kind {
                        SourceKind::Single(constructor) => {
                            let comp = constructor(
                                target_nodes.take().unwrap(),
                                channel_kind,
                                &mut self.ctx.pipeline.data_system(),
                            );

                            let source_manager = source_manager_cons(
                                dfg_node.config.state_id,
                                vec![comp],
                                &mut self.ctx.pipeline,
                            );
                        }
                        SourceKind::Parallel => {}
                    }
                }
                DFGNodeKind::Node(node_cons, manager_cons) => {
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

                    // Establish NodeManager for this Operator

                    let manager = manager_cons(
                        dfg_node.config.state_id.clone(),
                        vec![NodeID::new(0)],
                        &mut self.ctx.pipeline,
                    );

                    let node: Box<dyn std::any::Any> = node_cons(
                        String::from("node_1"), // Fix
                        NodeID::new(0),         // Fix
                        vec![NodeID::new(0)],   // Fix
                        components,
                        channel_kind,
                        self.ctx.pipeline.system(),
                        manager,
                    );

                    target_nodes = Some(vec![node]);
                }
            }
        }
        AssembledPipeline::new(self.ctx.pipeline)
    }

    pub(crate) fn new(ctx: Context) -> Self {
        Self {
            _marker: PhantomData,
            prev_dfg_id: DFGNodeID(0),
            ctx,
        }
    }
}
