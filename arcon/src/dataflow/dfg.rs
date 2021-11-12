use super::constructor::*;

/// A logical dataflow-graph.
#[allow(dead_code)]
#[allow(clippy::upper_case_acronyms)]
#[derive(Default)]
pub struct DFG {
    /// The graph is represented as a Vec for maximum space-efficiency.
    /// This works since nodes are never deleted from the graph.
    pub(crate) graph: Vec<DFGNode>,
}

unsafe impl Send for DFG {}

impl DFG {
    /// Inserts a [`DFGNode`] into the dataflow graph and returns a unique
    /// identifier of it.
    pub fn insert(&mut self, node: DFGNode) -> DFGNodeID {
        let id = DFGNodeID(self.graph.len());
        self.graph.push(node);
        id
    }

    /// Returns a reference to the [`DFGNode`] associated to a [`DFGNodeID`].
    #[allow(dead_code)]
    pub fn get(&self, id: &DFGNodeID) -> &DFGNode {
        self.graph.get(id.0).unwrap()
    }

    /// Returns a mutable reference to the [`DFGNode`] associated to a [`DFGNodeID`].
    #[allow(dead_code)]
    pub fn get_mut(&mut self, id: &DFGNodeID) -> &mut DFGNode {
        self.graph.get_mut(id.0).unwrap()
    }
}

/// The ID of a [`DFGNode`] in the dataflow graph.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct DFGNodeID(pub usize);

/// A logical node in the dataflow graph.
#[allow(dead_code)]
pub struct DFGNode {
    pub(crate) kind: DFGNodeKind,
    pub(crate) outgoing_channels: usize,
    pub(crate) ingoing_channels: usize,
    /// Ingoing edges to a node.
    pub(crate) ingoing: Vec<DFGNodeID>,
    pub(crate) channel_kind: ChannelKind,
}

impl DFGNode {
    pub fn new(
        kind: DFGNodeKind,
        outgoing_channels: usize,
        ingoing_channels: usize,
        ingoing: Vec<DFGNodeID>,
    ) -> Self {
        Self {
            kind,
            outgoing_channels,
            ingoing_channels,
            ingoing,
            channel_kind: Default::default(),
        }
    }
}

pub enum DFGNodeKind {
    Source(ChannelKind, SourceManagerConstructor),
    Node(NodeManagerConstructor),
}

#[allow(dead_code)]
pub enum SourceKind {
    Single(SourceConstructor),
    Parallel,
}

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

    assert_eq!(DFGNodeID(0), node0);
    assert_eq!(DFGNodeID(1), node1);
    assert_eq!(DFGNodeID(2), node2);
    assert_eq!(DFGNodeID(3), node3);
}
*/
