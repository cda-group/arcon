use crate::{
    application::conf::logger::ArconLogger,
    data::{ArconMessage, Epoch, NodeID, StateID, Watermark},
    dataflow::dfg::GlobalNodeId,
    error::*,
    index::EMPTY_STATE_ID,
    manager::snapshot::{Snapshot, SnapshotEvent, SnapshotManagerPort},
    prelude::OperatorBuilder,
    stream::operator::Operator,
};

#[cfg(feature = "metrics")]
use metrics::{gauge, register_gauge, register_histogram};

use arcon_state::Backend;
use fxhash::FxHashMap;
use kompact::{component::AbstractComponent, prelude::*};
use std::{collections::HashMap, sync::Arc};

pub type AbstractNode<IN> = (
    Arc<dyn AbstractComponent<Message = ArconMessage<IN>>>,
    RequiredRef<NodeManagerPort>,
);

#[cfg(feature = "metrics")]
#[derive(Debug, Clone)]
pub struct MetricReport {
    pub(crate) descriptor: String,
    pub(crate) id: NodeID,
    pub(crate) parallelism: usize,
}

/// Enum representing events that a Node may send to its manager
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum NodeManagerEvent {
    Watermark(NodeID, Watermark),
    Epoch(NodeID, Epoch),
    Checkpoint(NodeID, SnapshotEvent),
}

/// A [kompact] port for bidirectional communication between a Node and its NodeManager
pub struct NodeManagerPort {}
impl Port for NodeManagerPort {
    type Indication = ();
    type Request = NodeManagerEvent;
}

pub struct NodeManagerState {
    watermarks: HashMap<NodeID, Watermark>,
    epochs: HashMap<NodeID, Epoch>,
}

impl NodeManagerState {
    fn new() -> Self {
        Self {
            watermarks: HashMap::new(),
            epochs: HashMap::new(),
        }
    }
}

/// A [kompact] component responsible for coordinating a set of Arcon nodes
///
/// The following illustrates the role of a NodeManager in the context of a Application
///
/// ```text
///                  Application
///                /             \
///         NodeManager <----> NodeManager
///             |                  |
///          MapNode1  ------> WindowNode1
///             |                  |
///          MapNode2  ------> WindowNode2
///
/// ```
#[allow(dead_code)]
#[derive(ComponentDefinition)]
pub struct NodeManager<OP, B>
where
    OP: Operator + 'static,
    B: Backend,
{
    /// Component Context
    ctx: ComponentContext<Self>,
    /// A text description of the operating NodeManager
    ///
    /// e.g., window_sliding_avg_price
    pub(crate) state_id: StateID,
    /// Port for incoming local events from nodes this manager controls
    pub(crate) manager_port: ProvidedPort<NodeManagerPort>,
    /// Port for the SnapshotManager component
    pub(crate) snapshot_manager_port: RequiredPort<SnapshotManagerPort>,
    /// Reference to KompactSystem that the Nodes run on..
    data_system: KompactSystem,
    /// Current Node parallelism
    node_parallelism: usize,
    /// Max Node parallelism
    max_node_parallelism: usize,
    /// Current Node IDs that are connected to nodes on this manager
    in_channels: Vec<NodeID>,
    /// Monotonically increasing Node ID index
    node_index: u32,
    /// Active Nodes on this NodeManager
    pub(crate) nodes: FxHashMap<GlobalNodeId, AbstractNode<OP::IN>>,
    /// Internal manager state
    manager_state: NodeManagerState,
    latest_snapshot: Option<Snapshot>,
    builder: Arc<OperatorBuilder<OP, B>>,
    logger: ArconLogger,
}

impl<OP, B> NodeManager<OP, B>
where
    OP: Operator + 'static,
    B: Backend,
{
    pub fn new(
        state_id: String,
        data_system: KompactSystem,
        in_channels: Vec<NodeID>,
        logger: ArconLogger,
        builder: Arc<OperatorBuilder<OP, B>>,
    ) -> Self {
        #[cfg(feature = "metrics")]
        {
            register_gauge!("nodes", "node_manager" => state_id.clone());
            register_histogram!("checkpoint_execution_time_ms", "node_manager" => state_id.clone());
            register_gauge!("last_checkpoint_size", "node_manager"=> state_id.clone());
        }
        NodeManager {
            ctx: ComponentContext::uninitialised(),
            state_id,
            manager_port: ProvidedPort::uninitialised(),
            snapshot_manager_port: RequiredPort::uninitialised(),
            data_system,
            node_parallelism: num_cpus::get(),
            max_node_parallelism: (num_cpus::get() * 2) as usize,
            node_index: 0,
            in_channels,
            nodes: FxHashMap::<GlobalNodeId, AbstractNode<OP::IN>>::default(),
            manager_state: NodeManagerState::new(),
            latest_snapshot: None,
            logger,
            builder,
        }
    }

    /// Helper method to check if the NodeManager is responsible for any state
    /// that should go to the SnapshotManager.
    ///
    /// Empty ArconState () starts its STATE_ID with !
    #[inline]
    fn has_snapshot_state(&self) -> bool {
        self.state_id != EMPTY_STATE_ID
    }

    fn handle_node_event(&mut self, event: NodeManagerEvent) -> ArconResult<()> {
        match event {
            NodeManagerEvent::Watermark(id, w) => {
                self.manager_state.watermarks.insert(id, w);
            }
            NodeManagerEvent::Epoch(id, e) => {
                self.manager_state.epochs.insert(id, e);
            }
            NodeManagerEvent::Checkpoint(id, s) => {
                debug!(self.logger, "Reporting Checkpoint from Node ID {:?}", id);
                self.snapshot_manager_port.trigger(s);
            }
        }
        Ok(())
    }
}

impl<OP, B> ComponentLifecycle for NodeManager<OP, B>
where
    OP: Operator + 'static,
    B: Backend,
{
    fn on_start(&mut self) -> Handled {
        info!(self.logger, "Started NodeManager for {}", self.state_id,);

        #[cfg(feature = "metrics")]
        gauge!("nodes", self.nodes.len() as f64 ,"node_manager" => self.state_id.clone());
        // Register state id
        if self.has_snapshot_state() {
            self.snapshot_manager_port
                .trigger(SnapshotEvent::Register(self.state_id.clone()));
        }

        Handled::Ok
    }
}
impl<OP, B> Require<SnapshotManagerPort> for NodeManager<OP, B>
where
    OP: Operator + 'static,
    B: Backend,
{
    fn handle(&mut self, _: Never) -> Handled {
        unreachable!("can't be instantiated!");
    }
}

impl<OP, B> Provide<SnapshotManagerPort> for NodeManager<OP, B>
where
    OP: Operator + 'static,
    B: Backend,
{
    fn handle(&mut self, _: SnapshotEvent) -> Handled {
        Handled::Ok
    }
}

impl<OP, B> Provide<NodeManagerPort> for NodeManager<OP, B>
where
    OP: Operator + 'static,
    B: Backend,
{
    fn handle(&mut self, event: NodeManagerEvent) -> Handled {
        if let Err(err) = self.handle_node_event(event) {
            error!(
                self.logger,
                "Failed to handle NodeManagerEvent {:?}",
                err.to_string()
            );
        }

        Handled::Ok
    }
}

impl<OP, B> Actor for NodeManager<OP, B>
where
    OP: Operator + 'static,
    B: Backend,
{
    type Message = Never;

    fn receive_local(&mut self, _: Self::Message) -> Handled {
        Handled::Ok
    }
    fn receive_network(&mut self, _: NetMessage) -> Handled {
        unreachable!();
    }
}
