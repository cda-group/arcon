use super::constructor::*;
use crate::data::NodeID;
use std::rc::Rc;

pub type OperatorId = usize;

/// Logical Name, can be derived from a Deployment.
/// Resolveable to an `ActorPath` during runtime.
#[derive(Debug, Clone, PartialEq, std::cmp::Eq, std::hash::Hash, Copy)]
pub struct GlobalNodeId {
    pub operator_id: OperatorId,
    pub node_id: NodeID,
}

#[allow(dead_code)]
impl GlobalNodeId {
    // Helper function to make place-holder GlobalNodeId (used in testing etc.)
    pub fn null() -> GlobalNodeId {
        GlobalNodeId {
            operator_id: 0,
            node_id: NodeID::new(0),
        }
    }
}

/// A logical dataflow-graph.
#[allow(dead_code)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Default, Clone)]
pub struct DFG {
    /// The graph is represented as a Vec for maximum space-efficiency.
    /// This works since nodes are never deleted from the graph.
    pub(crate) graph: Vec<DFGNode>,
}

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

    /// Returns the ChannelKind
    #[allow(dead_code)]
    pub fn get_channel_kind(&self) -> &ChannelKind {
        &self.channel_kind
    }

    pub fn get_operator_id(&self) -> OperatorId {
        self.operator_id
    }
}

#[derive(Clone)]
pub enum DFGNodeKind {
    Source(Rc<dyn SourceFactory>),
    Node(Rc<dyn NodeFactory>),
    Placeholder,
}

#[derive(Clone, Copy, Debug)]
pub enum ChannelKind {
    /// If Operator A with parallelism P (nodes a1, a2, ..., aP) sends messages
    /// to Operator B with parallelism P (nodes b1, b2, ..., bP)
    /// node a1 sends to b1, a2 sends to b2 etc.
    Forward,
    /// If Operator A with parallelism P (nodes a1, a2, ..., aP) sends messages
    /// to Operator B with parallelism P (nodes b1, b2, ..., bP)
    /// all nodes executing A will broadcast each message to all nodes executing B.
    Broadcast,
    /// Unimplemented
    RoundRobin,
    /// Hashes elements according to a given key_extractor function and distributes the messages according to the hash.
    Keyed,
    /// Logs outgoing elements to the console without sending anything
    Console,
    /// Outgoing messages will be discarded
    Mute,
}

impl Default for ChannelKind {
    fn default() -> Self {
        ChannelKind::Forward
    }
}
