use std::any::Any;

/// A logical dataflow-graph.
#[derive(Default)]
pub struct DFG {
    /// The graph is represented as a Vec for maximum space-efficiency.
    /// This works since nodes are never deleted from the graph.
    graph: Vec<DFGNode>,
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
}

/// The ID of a [`DFGNode`] in the dataflow graph.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct DFGNodeID(usize);

/// A logical node in the dataflow graph.
pub struct DFGNode {
    kind: DFGNodeKind,
    /// Ingoing edges to a node.
    ingoing: Vec<DFGNodeID>,
    //     constructor: Box<dyn Fn(Vec<Box<dyn ChannelTrait>>) -> (Box<dyn ErasedNode>, Vec<Box<dyn ChannelTrait>>)>,
}

impl DFGNode {
    pub fn new(kind: DFGNodeKind, ingoing: Vec<DFGNodeID>) -> Self {
        Self { kind, ingoing }
    }
}

// OP::IN OP::OUT
pub enum DFGNodeKind {
    Source(Box<dyn Any>),
    Node(Box<dyn Any>),
}

#[test]
fn create_dfg() {
    use DFGNodeKind::*;
    let mut dfg = DFG::default();

    let node0 = dfg.insert(DFGNode::new(Node(Box::new(())), vec![]));
    let node1 = dfg.insert(DFGNode::new(Node(Box::new(())), vec![node0]));
    let node2 = dfg.insert(DFGNode::new(Node(Box::new(())), vec![node0]));
    let node3 = dfg.insert(DFGNode::new(Node(Box::new(())), vec![node1, node2]));

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
