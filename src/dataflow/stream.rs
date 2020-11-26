// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconType, NodeID},
    stream::operator::Operator,
    util::SafelySendableFn,
};
use arcon_error::ArconResult;
use arcon_state::index::ArconState;
use crate::prelude::ChannelStrategy;
use core::any::Any;
use std::{collections::HashMap, marker::PhantomData};

pub trait ChannelTrait {
    // whatever you like to put inside of your trait
}

use downcast::*;
downcast!(dyn ChannelTrait);

pub struct DFGNode {
    dfg_type: DFGType,
    ingoing: Vec<DFGNodeID>,
    //op_fn: Box<Fn(Vec<Box<dyn ChannelTrait>>) -> (Box<dyn ErasedNode>, Vec<Box<dyn ChannelTrait>>)>,
}
impl DFGNode {
    pub fn new(dfg_type: DFGType, ingoing: Vec<DFGNodeID>) -> Self {
        Self { dfg_type, ingoing }
    }
}

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

// OP::IN OP::OUT
pub enum DFGType {
    Source(Box<dyn Any>),
    Node(Box<dyn Any>),
}

use crate::prelude::Map;

pub type DFGNodeID = usize;


pub struct Stream<IN: ArconType> {
    _marker: PhantomData<IN>,
    previous_dfg_id: DFGNodeID,
    graph: HashMap<DFGNodeID, DFGNode>,
}

impl<IN: ArconType> Stream<IN> {
    pub fn map<OUT: ArconType, F: SafelySendableFn(IN) -> ArconResult<OUT>>(
        mut self,
        f: F,
    ) -> Stream<OUT> {
        // Operator, Backend, ChannelStrategy<A>
        //  Map <- (ChannelStrategy) <- Sink

        // Collection -> Map -> Sink
        // (ChannelStrategy, NodeID) -> Into Map Closure
        let closure = |mut channels: Vec<Box<dyn ChannelTrait>>| {
            let map = Map::new(f); // Operator impl
            let channel = channels.remove(0);
            let c: Box<ChannelStrategy<OUT>> = channel.downcast().unwrap();
            //let node = ERASEDNODE
            // return (Box<dyn ErasedNode>, Vec<Box<dyn ChannelTrait>>)
            
        };
        let boxed_closure = Box::new(closure);

        let previous = self.previous_dfg_id;
        let next_id = previous + 1;

        self.graph.insert(
            next_id,
            DFGNode::new(DFGType::Node(Box::new(())), vec![previous]),
        );
        Stream {
            _marker: PhantomData,
            previous_dfg_id: next_id,
            graph: self.graph,
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
        let source_id = 0;
        let source_dfg_node = DFGNode::new(DFGType::Source(Box::new(())), vec![]);
        let mut graph = HashMap::new();
        graph.insert(source_id, source_dfg_node);

        Self {
            _marker: PhantomData,
            previous_dfg_id: source_id,
            graph,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic]
    fn just_testing_things() {
        let stream: Stream<u64> = Stream::new();
        let new_stream = stream.map(|x| Ok(x + 1));
    }
}
