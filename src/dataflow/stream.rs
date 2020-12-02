// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconType, NodeID},
    dataflow::dfg::{DFGNode, DFGNodeID, DFGNodeKind, DFG},
    manager::state::StateID,
    prelude::{ChannelStrategy, Filter, FlatMap, Map, Node, NodeState},
    stream::operator::Operator,
    util::SafelySendableFn,
};
use arcon_error::ArconResult;
use arcon_state::{index::ArconState, BackendType};
use downcast::*;
use kompact::prelude::KompactSystem;
use std::{cell::RefCell, marker::PhantomData, rc::Rc, sync::Arc};

pub trait ChannelTrait {}
downcast!(dyn ChannelTrait);

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
    // Kompact-stuff
}

pub struct Stream<IN: ArconType> {
    _marker: PhantomData<IN>,
    /// ID of the node which outputs this stream.
    prev_dfg_id: DFGNodeID,
    ctx: Rc<RefCell<Context>>,
}

impl<IN: ArconType> Stream<IN> {
    /// Adds a Map transformation to the dataflow graph
    pub fn map<OUT: ArconType, F: 'static + SafelySendableFn(IN) -> ArconResult<OUT>>(
        &self,
        f: F,
    ) -> Stream<OUT> {
        self.operator(Map::new(f))
    }

    /// Adds a stateful Map transformation to the dataflow graph
    ///
    /// A state constructor `SC` must be defined and passed in.
    pub fn map_with_state<OUT, S, SC, F>(&self, f: F, sc: SC) -> Stream<OUT>
    where
        OUT: ArconType,
        S: ArconState,
        SC: SafelySendableFn() -> S,
        F: 'static + SafelySendableFn(IN, &mut S) -> ArconResult<OUT>,
    {
        self.operator(Map::stateful(sc(), f))
    }

    /// Adds a Filter transformation to the dataflow graph
    pub fn filter<F: 'static + SafelySendableFn(&IN) -> bool>(&self, f: F) -> Stream<IN> {
        self.operator(Filter::new(f))
    }

    /// Adds a stateful Filter transformation to the dataflow graph
    ///
    /// A state constructor `SC` must be defined and passed in.
    pub fn filter_with_state<S, SC, F>(&self, f: F, sc: SC) -> Stream<IN>
    where
        S: ArconState,
        SC: SafelySendableFn() -> S,
        F: 'static + SafelySendableFn(&IN, &mut S) -> bool,
    {
        self.operator(Filter::stateful(sc(), f))
    }

    /// Adds a FlatMap transformation to the dataflow graph
    pub fn flatmap<OUT, OUTS, F>(&self, f: F) -> Stream<OUTS::Item>
    where
        OUT: ArconType,
        OUTS: IntoIterator + 'static,
        OUTS::Item: ArconType,
        F: 'static + SafelySendableFn(IN) -> ArconResult<OUTS>,
    {
        self.operator(FlatMap::new(f))
    }

    /// Adds a stateful FlatMap transformation to the dataflow graph
    ///
    /// A state constructor `SC` must be defined and passed in.
    pub fn flatmap_with_state<OUT, OUTS, S, SC, F>(&self, f: F, sc: SC) -> Stream<OUTS::Item>
    where
        OUT: ArconType,
        OUTS: IntoIterator + 'static,
        OUTS::Item: ArconType,
        S: ArconState,
        SC: SafelySendableFn() -> S,
        F: 'static + SafelySendableFn(IN, &mut S) -> ArconResult<OUTS>,
    {
        self.operator(FlatMap::stateful(sc(), f))
    }

    /// This method may be used to add a custom defined [`Operator`] to the dataflow graph
    pub fn operator<OP: Operator + 'static>(&self, operator: OP) -> Stream<OP::OUT> {
        let constructor = node_cons!(operator, OP::OUT);

        let next_dfg_id = self
            .ctx
            .borrow_mut()
            .dfg
            .insert(DFGNode::new(DFGNodeKind::Node(constructor), vec![
                self.prev_dfg_id,
            ]));

        Stream {
            _marker: PhantomData,
            prev_dfg_id: next_dfg_id,
            ctx: self.ctx.clone(),
        }
    }

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

    pub fn new() -> Self {
        let mut dfg = DFG::default();
        let node = DFGNode::new(DFGNodeKind::Source(Box::new(())), vec![]);
        let id = dfg.insert(node);
        Self {
            _marker: PhantomData,
            prev_dfg_id: id,
            ctx: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn just_testing_things() {
        fn mapper(x: u64) -> ArconResult<u64> {
            Ok(x + 1)
        }

        fn filter_fn(x: &u64) -> bool {
            *x > 0
        }

        let stream0: Stream<u64> = Stream::new();
        let _stream1 = stream0.map(Box::new(|x| Ok(x + 1)));
        let _stream2 = stream0
            .map(mapper)
            .with_backend(BackendType::Sled)
            .with_state_id("map_state")
            .filter(filter_fn);
    }
}
