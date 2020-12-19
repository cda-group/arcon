use super::constructor::*;
use crate::{
    data::{NodeID, StateID},
    pipeline::Pipeline,
    stream::source::SourceEvent,
};
use kompact::{component::AbstractComponent, prelude::KompactSystem, Never};
use std::{any::Any, sync::Arc};

/// A logical dataflow-graph.
#[allow(dead_code)]
#[derive(Default)]
pub struct DFG {
    /// The graph is represented as a Vec for maximum space-efficiency.
    /// This works since nodes are never deleted from the graph.
    pub(crate) graph: Vec<DFGNode>,
}

impl DFG {
    /// Inserts a [`DFGNode`] into the dataflow graph and returns a unique
    /// identifier of it.
    pub fn insert(&mut self, node: DFGNode) -> DFGNodeID {
        let id = DFGNodeID(self.graph.len());
        self.graph.push(node);
        id
    }

    /// Returns a reference to the [`DFGNode`] associated to a [`DFGNodeID`].
    pub fn get(&self, id: &DFGNodeID) -> &DFGNode {
        self.graph.get(id.0).unwrap()
    }

    /// Returns a mutable reference to the [`DFGNode`] associated to a [`DFGNodeID`].
    pub fn get_mut(&mut self, id: &DFGNodeID) -> &mut DFGNode {
        self.graph.get_mut(id.0).unwrap()
    }

    /// Returns the length of the graph
    pub fn len(&self) -> usize {
        self.graph.len()
    }
}

/// The ID of a [`DFGNode`] in the dataflow graph.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct DFGNodeID(pub usize);

#[derive(Debug, Clone)]
pub struct OperatorConfig {
    /// State ID for this DFG node
    ///
    /// Default ID is a random generated one
    pub(crate) state_id: StateID,
}

impl OperatorConfig {
    /// Modifies the [`StateID`] of this logical DFGNode
    pub fn set_state_id<I: Into<StateID>>(&mut self, i: I) {
        self.state_id = i.into();
    }
}

impl Default for OperatorConfig {
    fn default() -> Self {
        Self {
            state_id: format!("op_{}", uuid::Uuid::new_v4().to_string()),
        }
    }
}

/// A logical node in the dataflow graph.
#[allow(dead_code)]
pub struct DFGNode {
    pub(crate) kind: DFGNodeKind,
    /// Ingoing edges to a node.
    ingoing: Vec<DFGNodeID>,
    /// Operator Configuration for this node
    pub(crate) config: OperatorConfig,
    pub(crate) channel_kind: ChannelKind,
}

impl DFGNode {
    pub fn new(kind: DFGNodeKind, config: OperatorConfig, ingoing: Vec<DFGNodeID>) -> Self {
        Self {
            kind,
            ingoing,
            config,
            channel_kind: Default::default(),
        }
    }
    pub fn is_source(&self) -> bool {
        match self.kind {
            DFGNodeKind::Source(_, _, _) => true,
            _ => false,
        }
    }
}

pub enum DFGNodeKind {
    Source(SourceKind, ChannelKind, SourceManagerConstructor),
    Node(NodeConstructor, NodeManagerConstructor),
}

pub enum SourceKind {
    Single(SourceConstructor),
    Parallel,
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum ChannelKind {
    Forward,
    Broadcast,
    RoundRobin,
    KeyBy,
    Console,
    Mute,
}

impl Default for ChannelKind {
    fn default() -> Self {
        ChannelKind::Forward
    }
}

// TODO: Fix
/*
#[test]
fn create_dfg() {
    use DFGNodeKind::*;
    let mut dfg = DFG::default();

    let node0 = dfg.insert(DFGNode::new(
        Node(Box::new(())),
        OperatorConfig::default(),
        vec![],
    ));
    let node1 = dfg.insert(DFGNode::new(
        Node(Box::new(())),
        OperatorConfig::default(),
        vec![node0],
    ));
    let node2 = dfg.insert(DFGNode::new(
        Node(Box::new(())),
        OperatorConfig::default(),
        vec![node0],
    ));
    let node3 = dfg.insert(DFGNode::new(
        Node(Box::new(())),
        OperatorConfig::default(),
        vec![node1, node2],
    ));

    // Constructs the dataflow graph:
    //
    //        +-> node1 -+
    //        |          |
    // node0 -+          +-> node3
    //        |          |
    //        +-> node2 -+

    assert_eq!(DFGNodeID(0), node0);
    assert_eq!(DFGNodeID(1), node1);
    assert_eq!(DFGNodeID(2), node2);
    assert_eq!(DFGNodeID(3), node3);
}
*/
