use super::constructor::*;
use crate::control_plane::distributed::OperatorId;
use crate::data::NodeID;

use crate::prelude::Arc;

/// A logical dataflow-graph.
#[allow(dead_code)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Default, Clone)]
pub struct DFG {
    /// The graph is represented as a Vec for maximum space-efficiency.
    /// This works since nodes are never deleted from the graph.
    pub(crate) graph: Vec<DFGNode>,
}

unsafe impl Send for DFG {}

impl DFG {
    /// Inserts a [`DFGNode`] into the dataflow graph and returns a unique
    /// identifier of it.
    pub fn insert(&mut self, node: DFGNode) -> OperatorId {
        let id = self.graph.len();
        self.graph.push(node);
        id
    }

    /// Returns a reference to the [`DFGNode`] associated to a [`OperatorId`].
    #[allow(dead_code)]
    pub fn get(&self, id: &OperatorId) -> &DFGNode {
        self.graph.get(*id).unwrap()
    }

    /// Returns a mutable reference to the [`DFGNode`] associated to a [`OperatorId`].
    #[allow(dead_code)]
    pub fn get_mut(&mut self, id: &OperatorId) -> &mut DFGNode {
        self.graph.get_mut(*id).unwrap()
    }
}

// /// The ID of a [`DFGNode`] in the dataflow graph.
// #[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
// pub struct OperatorId(pub usize);

/// A logical node in the dataflow graph.
#[allow(dead_code)]
#[derive(Clone)]
pub struct DFGNode {
    pub kind: DFGNodeKind,
    operator_id: OperatorId,
    paralellism: usize,
    outgoing_channels: Vec<NodeID>,
    /// Ingoing edges to a node.
    ingoing: Vec<NodeID>,
    channel_kind: ChannelKind,
}

impl DFGNode {
    pub fn new(
        kind: DFGNodeKind,
        operator_id: OperatorId,
        paralellism: usize,
        ingoing: Vec<NodeID>,
    ) -> Self {
        Self {
            kind,
            operator_id,
            paralellism,
            outgoing_channels: Vec::new(),
            ingoing,
            channel_kind: Default::default(),
        }
    }

    /// Returns a vector of NodeID's which this node will receive messages from
    pub fn get_input_channels(&self) -> &Vec<NodeID> {
        &self.ingoing
    }

    /// Returns a vector of NodeID's for this node
    pub fn get_node_ids(&self) -> Vec<NodeID> {
        (0..self.paralellism)
            .map(|i| NodeID::new(i as u32))
            .collect()
    }

    /// Sets the number of outgoing channels from this node
    pub fn set_outgoing_channels(&mut self, outgoing_channels: Vec<NodeID>) {
        self.outgoing_channels = outgoing_channels;
    }

    /// Configures the outgoing ChannelKind for this nodes Factory
    pub fn set_channel_kind(&mut self, channel_kind: ChannelKind) {
        match &mut self.kind {
            DFGNodeKind::Source(source_factory) => {
                todo!();
                // mutability issue...
                //source_factory.set_channel_kind(channel_kind);
            }
            DFGNodeKind::Node(node_factory) => {
                todo!();
                // node_factory.set_channel_kind(channel_kind);
            }
        }
    }

    /// Returns the ChannelKind
    pub fn get_channel_kind(&self) -> &ChannelKind {
        &self.channel_kind
    }

    pub fn get_operator_id(&self) -> OperatorId {
        self.operator_id
    }
}

#[derive(Clone)]
pub enum DFGNodeKind {
    Source(Arc<dyn SourceFactory>),
    Node(Arc<dyn NodeFactory>),
}
/*
#[allow(dead_code)]
pub enum SourceKind {
    Single(SourceConstructor),
    Parallel,
}
*/

#[derive(Clone, Copy, Debug)]
#[allow(dead_code)]
pub enum ChannelKind {
    Forward,
    Broadcast,
    RoundRobin,
    Keyed,
    Console,
    Mute,
}

impl Default for ChannelKind {
    fn default() -> Self {
        ChannelKind::Keyed
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

    assert_eq!(OperatorId(0), node0);
    assert_eq!(OperatorId(1), node1);
    assert_eq!(OperatorId(2), node2);
    assert_eq!(OperatorId(3), node3);
}
*/
