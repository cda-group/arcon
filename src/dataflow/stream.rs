// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconType, NodeID},
    dataflow::dfg::{DFGNode, DFGNodeID, DFGNodeKind, OperatorConfig, DFG},
    pipeline::Pipeline,
    prelude::{ChannelStrategy, Filter, FlatMap, Map, MapInPlace, Node, NodeState},
    stream::operator::Operator,
    util::SafelySendableFn,
};
use arcon_error::OperatorResult;
use arcon_state::{index::ArconState, BackendType};
use downcast::*;
use std::{marker::PhantomData, sync::Arc};

pub trait ChannelTrait {}
downcast!(dyn ChannelTrait);

// TODO: Fix this.
macro_rules! node_cons {
    ($op:expr, $out_type:ty) => {
        Box::new(|backend_type: BackendType| {
            arcon_state::with_backend_type!(backend_type, |SB| {
                Box::new(
                    |descriptor: String,
                     node_id: NodeID,
                     in_channels: Vec<NodeID>,
                     strategy: Box<dyn ChannelTrait>,
                     backend: Arc<SB>,
                     system: &KompactSystem| {
                        let channel_strategy: ChannelStrategy<$out_type> = *strategy.downcast().unwrap();
                        let node = Node::new(
                            descriptor,
                            channel_strategy,
                            $op,
                            NodeState::new(node_id, in_channels, backend),
                        );
                        let node_comp = system.create_erased(Box::new(node));
                        // TODO: start_notify...
                        system.start(&node_comp);
                        Box::new(node_comp) as Box<dyn Any> // Cast Arc<AbstractComponent<Message = ArconMessage<IN>> to Box<Any>
                    },
                )
            })
        })
    };
}

#[derive(Default)]
pub struct Context {
    dfg: DFG,
    pipeline: Pipeline,
}

impl Context {
    pub fn new(pipeline: Pipeline) -> Self {
        Self {
            dfg: Default::default(),
            pipeline,
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
        F: 'static + SafelySendableFn(IN) -> OUT,
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
        F: 'static + SafelySendableFn(IN, &mut S) -> OperatorResult<OUT>,
        C: FnOnce(&mut OperatorConfig),
    {
        let mut c = OperatorConfig::default();
        conf(&mut c);
        self.operator(Map::stateful(sc(), f), c)
    }

    /// Adds an in-place Map transformation to the dataflow graph
    pub fn map_in_place<F: 'static + SafelySendableFn(&mut IN)>(self, f: F) -> Stream<IN> {
        self.operator(MapInPlace::new(f), OperatorConfig::default())
    }

    /// Adds a stateful in-place Map transformation to the dataflow graph
    ///
    /// A state constructor `SC` must be defined and passed in.
    pub fn map_in_place_with_state<S, SC, F, C>(self, f: F, sc: SC, conf: C) -> Stream<IN>
    where
        S: ArconState,
        SC: SafelySendableFn() -> S,
        F: 'static + SafelySendableFn(&mut IN, &mut S) -> OperatorResult<()>,
        C: FnOnce(&mut OperatorConfig),
    {
        let mut c = OperatorConfig::default();
        conf(&mut c);
        self.operator(MapInPlace::stateful(sc(), f), c)
    }

    /// Adds a Filter transformation to the dataflow graph
    pub fn filter<F: 'static + SafelySendableFn(&IN) -> bool>(self, f: F) -> Stream<IN> {
        self.operator(Filter::new(f), OperatorConfig::default())
    }

    /// Adds a stateful Filter transformation to the dataflow graph
    ///
    /// A state constructor `SC` must be defined and passed in.
    pub fn filter_with_state<S, SC, F, C>(self, f: F, sc: SC, conf: C) -> Stream<IN>
    where
        S: ArconState,
        SC: SafelySendableFn() -> S,
        F: 'static + SafelySendableFn(&IN, &mut S) -> bool,
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
        F: 'static + SafelySendableFn(IN) -> OUTS,
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
        F: 'static + SafelySendableFn(IN, &mut S) -> OperatorResult<OUTS>,
        C: FnOnce(&mut OperatorConfig),
    {
        let mut c = OperatorConfig::default();
        conf(&mut c);
        self.operator(FlatMap::stateful(sc(), f), c)
    }

    /// This method may be used to add a custom defined [`Operator`] to the dataflow graph
    pub fn operator<OP: Operator + 'static>(
        mut self,
        operator: OP,
        conf: OperatorConfig,
    ) -> Stream<OP::OUT> {
        // TODO
        let constructor = Box::new(());

        let next_dfg_id =
            self.ctx
                .dfg
                .insert(DFGNode::new(DFGNodeKind::Node(constructor), conf, vec![
                    self.prev_dfg_id,
                ]));

        Stream {
            _marker: PhantomData,
            prev_dfg_id: next_dfg_id,
            ctx: self.ctx,
        }
    }

    /*
    /// Set [`BackendType`] to the previous node in the graph
    pub fn with_backend(&mut self, backend_type: BackendType) -> Stream<IN> {
        self.ctx
            .borrow_mut()
            .dfg
            .get_mut(&self.prev_dfg_id)
            .set_backend_type(backend_type);

        Stream {
            _marker: PhantomData,
            prev_dfg_id: self.prev_dfg_id,
            ctx: self.ctx.clone(),
        }
    }

    /// Set [`StateID`] to the previous node in the graph
    pub fn with_state_id<I>(&mut self, id: I) -> Stream<IN>
    where
        I: Into<StateID>,
    {
        self.ctx
            .borrow_mut()
            .dfg
            .get_mut(&self.prev_dfg_id)
            .set_state_id(id.into());

        Stream {
            _marker: PhantomData,
            prev_dfg_id: self.prev_dfg_id,
            ctx: self.ctx.clone(),
        }
    }
    */

    /// Builds the Dataflow graph
    ///
    /// Returns a [`Pipeline`] where all runtime components
    /// have been conneted and started.
    ///
    /// Note that this method only builds the pipeline. In order
    /// to start it, see the following [method](Pipeline::start).
    pub fn build(self) -> Pipeline {
        // TODO: Iterate over Dataflow graph
        //       and connect components!
        self.ctx.pipeline
    }

    pub(crate) fn new(ctx: Context) -> Self {
        let mut dfg = DFG::default();
        let conf = OperatorConfig::default();
        let node = DFGNode::new(DFGNodeKind::Source(Box::new(())), conf, vec![]);
        let id = dfg.insert(node);
        Self {
            _marker: PhantomData,
            prev_dfg_id: id,
            ctx,
        }
    }
}
