use crate::manager::state::StateID;
use arcon_state::BackendType;
use std::any::Any;

/// A logical dataflow-graph.
#[allow(dead_code)]
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

#[derive(Debug)]
pub struct OperatorConfig {
    /// State Backend type to use for this DFG Node
    ///
    /// Default backend is Sled
    backend_type: BackendType,
    /// State ID for this DFG node
    ///
    /// Default ID is a random generated one
    state_id: StateID,
}

impl OperatorConfig {
    /// Modifies the [`BackendType`] of this logical DFGNode
    pub fn set_backend(&mut self, backend_type: BackendType) {
        self.backend_type = backend_type;
    }
    /// Modifies the [`StateID`] of this logical DFGNode
    pub fn set_state_id<I: Into<StateID>>(&mut self, i: I) {
        self.state_id = i.into();
    }
}

impl Default for OperatorConfig {
    fn default() -> Self {
        Self {
            #[cfg(feature = "rocksdb")]
            backend_type: BackendType::Rocks,
            #[cfg(not(feature = "rocksdb"))]
            backend_type: Default::default(),
            state_id: format!("op_{}", uuid::Uuid::new_v4().to_string()),
        }
    }
}

/// A logical node in the dataflow graph.
#[allow(dead_code)]
pub struct DFGNode {
    kind: DFGNodeKind,
    /// Ingoing edges to a node.
    ingoing: Vec<DFGNodeID>,
    config: OperatorConfig,
    // TODO: constructor
}

impl DFGNode {
    pub fn new(kind: DFGNodeKind, config: OperatorConfig, ingoing: Vec<DFGNodeID>) -> Self {
        Self {
            kind,
            ingoing,
            config,
        }
    }
}

pub enum DFGNodeKind {
    Source(Box<dyn Any>),
    Node(Box<dyn Any>),
}

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
