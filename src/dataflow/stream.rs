// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::ArconType,
    dataflow::dfg::{DFGNode, DFGNodeID, DFGNodeKind, DFG},
    prelude::ChannelStrategy,
    stream::operator::Operator,
    util::SafelySendableFn,
};
use arcon_error::ArconResult;
use arcon_state::index::ArconState;
use std::{cell::RefCell, marker::PhantomData, rc::Rc};

pub trait ChannelTrait {
    // whatever you like to put inside of your trait
}

use downcast::*;
downcast!(dyn ChannelTrait);

// NOTES.
//
// Configurables:
//
// (1) State Backend; (2) State ID; (3) Parallelism
//
// With Defaults: (1) BackendNever; (3) num_cpus::defaut()?
// Must be chosen?: (2) State ID
//
// (1): stream.map(..).with_backend(RocksDB)
// (2): stream.map(..).state_id("map_state")
// (3): stream.map(..).parallelism(12)

#[derive(Default)]
pub struct Context {
    dfg: DFG,
    // Kompact-stuff
}

use crate::prelude::Map;

pub struct Stream<IN: ArconType> {
    _marker: PhantomData<IN>,
    /// ID of the node which outputs this stream.
    prev_dfg_id: DFGNodeID,
    ctx: Rc<RefCell<Context>>,
}

impl<IN: ArconType> Stream<IN> {
    pub fn map<OUT: ArconType, F: 'static + SafelySendableFn(IN) -> ArconResult<OUT>>(
        &self,
        f: F,
    ) -> Stream<OUT> {
        // Operator, Backend, ChannelStrategy<A>
        //  Map <- (ChannelStrategy) <- Sink

        // Collection -> Map -> Sink
        // (ChannelStrategy, NodeID) -> Into Map Closure
        let constructor = Box::new(|mut channels: Vec<Box<dyn ChannelTrait>>| {
            let operator = Map::new(f); // Operator impl
            let channel: Box<ChannelStrategy<OUT>> = channels.remove(0).downcast().unwrap();
            //let node = ERASEDNODE
            // return (Box<dyn ErasedNode>, Vec<Box<dyn ChannelTrait>>)
        });

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

    pub fn map_with_state<OUT, S, F>(self, f: F) -> Stream<OUT>
    where
        OUT: ArconType,
        S: ArconState,
        F: Fn(IN, &mut S) -> ArconResult<OUT>,
    {
        unimplemented!();
    }

    pub fn operator<OP: Operator>(self, operator: OP) -> Stream<OP::OUT> {
        unimplemented!();
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
    #[should_panic]
    fn just_testing_things() {
        fn mapper(x: u64) -> ArconResult<u64> {
            Ok(x + 1)
        }

        let stream0: Stream<u64> = Stream::new();
        let _stream1 = stream0.map(Box::new(|x| Ok(x + 1)));
        let _stream2 = stream0.map(mapper);
    }
}
