// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

#[cfg(feature = "arcon_arrow")]
use crate::data::arrow::ImmutableTable;
#[cfg(feature = "arcon_arrow")]
use crate::index::ArconState;
#[cfg(feature = "arcon_arrow")]
use crate::manager::query::{QueryManagerMsg, QueryManagerPort, TableRegistration};
use crate::{
    data::{ArconMessage, Epoch, NodeID, StateID, Watermark},
    index::{HashTable, IndexOps, LocalValue, StateConstructor, ValueIndex, EMPTY_STATE_ID},
    manager::{
        epoch::EpochEvent,
        snapshot::{Snapshot, SnapshotEvent, SnapshotManagerPort},
    },
    stream::operator::Operator,
};
use arcon_error::*;
use arcon_macros::ArconState;
use arcon_state::Backend;
use fxhash::FxHashMap;
use kompact::{component::AbstractComponent, prelude::*};
use std::{collections::HashSet, sync::Arc};

#[cfg(feature = "metrics")]
use crate::stream::node::NodeMetrics;

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
    pub(crate) metrics: NodeMetrics,
}

/// Checkpoint Request for a running Node
#[derive(Clone, Debug)]
pub struct CheckpointRequest {
    /// Indicates which Node the request is coming from
    pub(crate) id: NodeID,
    /// Which Epoch the request is for
    pub(crate) epoch: Epoch,
}

impl CheckpointRequest {
    pub fn new(id: NodeID, epoch: Epoch) -> Self {
        Self { id, epoch }
    }
}

#[derive(Clone, Debug)]
pub enum CheckpointResponse {
    /// Nothing has changed, continue as normal.
    NoAction,
}

/// Enum representing events that the Manager may send back to a Node
#[derive(Clone, Debug)]
pub enum NodeEvent {
    CheckpointResponse(CheckpointResponse),
}

/// Enum representing events that a Node may send to its manager
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum NodeManagerEvent {
    #[cfg(feature = "metrics")]
    Metrics(NodeID, NodeMetrics),
    Watermark(NodeID, Watermark),
    Epoch(NodeID, Epoch),
    Checkpoint(CheckpointRequest),
}

/// A [kompact] port for bidirectional communication between a Node and its NodeManager
pub struct NodeManagerPort {}
impl Port for NodeManagerPort {
    type Indication = NodeEvent;
    type Request = NodeManagerEvent;
}

#[derive(ArconState)]
pub struct NodeManagerState<B: Backend> {
    watermarks: HashTable<NodeID, Watermark, B>,
    epochs: HashTable<NodeID, Epoch, B>,
    current_watermark: LocalValue<Watermark, B>,
    current_epoch: LocalValue<Epoch, B>,
    #[ephemeral]
    checkpoint_acks: HashSet<(NodeID, Epoch)>,
}

impl<B: Backend> StateConstructor for NodeManagerState<B> {
    type BackendType = B;
    fn new(backend: Arc<Self::BackendType>) -> Self {
        Self {
            watermarks: HashTable::with_capacity("_watermarks", backend.clone(), 64, 64),
            epochs: HashTable::with_capacity("_epochs", backend.clone(), 64, 64),
            current_watermark: LocalValue::new("_curr_watermark", backend.clone()),
            current_epoch: LocalValue::new("_curr_epoch", backend.clone()),
            checkpoint_acks: HashSet::new(),
        }
    }
}

/// A [kompact] component responsible for coordinating a set of Arcon nodes
///
/// The following illustrates the role of a NodeManager in the context of a Pipeline
///
/// ```text
///                    Pipeline
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
    state_id: StateID,
    /// Port for incoming local events from nodes this manager controls
    pub(crate) manager_port: ProvidedPort<NodeManagerPort>,
    /// Port for the SnapshotManager component
    pub(crate) snapshot_manager_port: RequiredPort<SnapshotManagerPort>,
    /// Port for the QueryManager component
    #[cfg(feature = "arcon_arrow")]
    pub(crate) query_manager_port: RequiredPort<QueryManagerPort>,
    /// Actor Reference to the EpochManager
    epoch_manager: ActorRefStrong<EpochEvent>,
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
    pub(crate) nodes: FxHashMap<NodeID, AbstractNode<OP::IN>>,
    /// State Backend used to persist data
    backend: Arc<B>,
    /// Internal manager state
    manager_state: NodeManagerState<B>,
    latest_snapshot: Option<Snapshot>,
}

impl<OP, B> NodeManager<OP, B>
where
    OP: Operator + 'static,
    B: Backend,
{
    pub fn new(
        state_id: String,
        data_system: KompactSystem,
        epoch_manager: ActorRefStrong<EpochEvent>,
        in_channels: Vec<NodeID>,
        backend: Arc<B>,
    ) -> Self {
        NodeManager {
            ctx: ComponentContext::uninitialised(),
            state_id,
            manager_port: ProvidedPort::uninitialised(),
            snapshot_manager_port: RequiredPort::uninitialised(),
            #[cfg(feature = "arcon_arrow")]
            query_manager_port: RequiredPort::uninitialised(),
            epoch_manager,
            data_system,
            node_parallelism: num_cpus::get(),
            max_node_parallelism: (num_cpus::get() * 2) as usize,
            node_index: 0,
            in_channels,
            nodes: FxHashMap::default(),
            manager_state: NodeManagerState::new(backend.clone()),
            backend,
            latest_snapshot: None,
        }
    }

    #[inline]
    fn checkpoint(&mut self) -> ArconResult<()> {
        if let Some(base_dir) = &self.ctx.config()["checkpoint_dir"].as_string() {
            let curr_epoch = match self.manager_state.current_epoch().get()? {
                Some(v) => v.as_ref().epoch,
                None => return arcon_err!("failed to fetch epoch"),
            };

            let checkpoint_dir = format!(
                "{}/checkpoint_{id}_{epoch}",
                base_dir,
                id = self.state_id,
                epoch = curr_epoch,
            );

            self.backend.checkpoint(checkpoint_dir.as_ref())?;

            // Send snapshot to SnapshotManager
            if self.has_snapshot_state() {
                let snapshot = Snapshot::new(
                    std::any::type_name::<B>().to_string(),
                    curr_epoch,
                    checkpoint_dir.clone(),
                );

                self.snapshot_manager_port.trigger(SnapshotEvent::Snapshot(
                    self.state_id.clone(),
                    snapshot.clone(),
                ));

                self.latest_snapshot = Some(snapshot);
            }

            // Send Ack to EpochManager
            self.epoch_manager.tell(EpochEvent::Ack(
                self.state_id.clone(),
                Epoch::new(curr_epoch),
            ));

            // bump epoch
            self.manager_state.current_epoch().rmw(|e| {
                e.epoch += 1;
            })?;

            debug!(
                self.ctx.log(),
                "Completed a Checkpoint to path {}", checkpoint_dir
            );
        } else {
            return arcon_err!("Failed to fetch checkpoint_dir from Config");
        }

        Ok(())
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
                self.manager_state.watermarks.put(id, w)?;
            }
            NodeManagerEvent::Epoch(id, e) => {
                self.manager_state.epochs.put(id, e)?;
            }
            NodeManagerEvent::Checkpoint(request) => {
                if self.nodes.contains_key(&request.id) {
                    let epoch = match self.manager_state.current_epoch().get()? {
                        Some(v) => v.into_owned(),
                        None => return arcon_err!("failed to fetch epoch"),
                    };
                    if request.epoch == epoch {
                        self.manager_state
                            .checkpoint_acks
                            .insert((request.id, request.epoch));

                        if self.manager_state.checkpoint_acks.len() == self.nodes.len() {
                            self.checkpoint()?;
                            self.manager_state.checkpoint_acks.clear();

                            for (_, (_, port_ref)) in &self.nodes {
                                self.data_system.trigger_i(
                                    NodeEvent::CheckpointResponse(CheckpointResponse::NoAction),
                                    &port_ref,
                                );
                            }

                            #[cfg(feature = "arcon_arrow")]
                            {
                                if OP::OperatorState::has_tables() {
                                    if let Some(snapshot) = &self.latest_snapshot {
                                        let mut state =
                                            OP::OperatorState::restore(snapshot.clone())?;
                                        for table in state.tables() {
                                            let imut = ImmutableTable::from(table);
                                            let registration = TableRegistration {
                                                epoch: epoch.epoch,
                                                table: imut,
                                            };
                                            self.query_manager_port.trigger(
                                                QueryManagerMsg::TableRegistration(registration),
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            #[cfg(feature = "metrics")]
            NodeManagerEvent::Metrics(_, _) => {}
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
        info!(self.ctx.log(), "Started NodeManager for {}", self.state_id,);

        // Register state id
        if self.has_snapshot_state() {
            self.snapshot_manager_port
                .trigger(SnapshotEvent::Register(self.state_id.clone()));
        }

        self.epoch_manager
            .tell(EpochEvent::Register(self.state_id.clone()));

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

#[cfg(feature = "arcon_arrow")]
impl<OP, B> Require<QueryManagerPort> for NodeManager<OP, B>
where
    OP: Operator + 'static,
    B: Backend,
{
    fn handle(&mut self, _: Never) -> Handled {
        unreachable!("can't be instantiated!");
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
                self.ctx.log(),
                "Failed to handle NodeManagerEvent {:?}", err
            );
        }

        Handled::Ok
    }
}

impl<OP, B> Require<NodeManagerPort> for NodeManager<OP, B>
where
    OP: Operator + 'static,
    B: Backend,
{
    fn handle(&mut self, _: NodeEvent) -> Handled {
        unreachable!("Not supposed to be called");
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
