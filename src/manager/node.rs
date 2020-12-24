// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{Epoch, NodeID, StateID, Watermark},
    manager::{
        epoch::EpochEvent,
        snapshot::{Snapshot, SnapshotEvent, SnapshotManagerPort},
    },
};
use arcon_error::*;
use arcon_state::{ArconState, Backend, Handle, Map, Value};
use kompact::prelude::*;
use std::sync::Arc;

#[cfg(feature = "metrics")]
use crate::stream::node::NodeMetrics;

#[cfg(feature = "metrics")]
#[derive(Debug, Clone)]
pub struct MetricReport {
    pub(crate) descriptor: String,
    pub(crate) id: NodeID,
    pub(crate) parallelism: usize,
    pub(crate) metrics: NodeMetrics,
}
use std::sync::atomic::{AtomicBool, Ordering};

/// Checkpoint Request for a running Node
#[derive(Debug)]
pub struct CheckpointRequest {
    /// Indicates which Node the request is coming from
    pub(crate) id: NodeID,
    /// Which Epoch the request is for
    pub(crate) epoch: Epoch,
    /// A flag for indicating whether the checkpoint completed
    flag: AtomicBool,
}

impl CheckpointRequest {
    pub fn new(id: NodeID, epoch: Epoch) -> Self {
        Self {
            id,
            epoch,
            flag: AtomicBool::new(false),
        }
    }
    fn complete(&self) {
        self.flag.store(true, Ordering::Relaxed);
    }
    pub(crate) fn is_complete(&self) -> bool {
        self.flag.load(Ordering::Relaxed)
    }
}

/// Enum containing possible local node events
#[derive(Debug)]
pub enum NodeEvent {
    #[cfg(feature = "metrics")]
    Metrics(NodeID, NodeMetrics),
    Watermark(NodeID, Watermark),
    Epoch(NodeID, Epoch),
    Checkpoint(Arc<CheckpointRequest>),
}

impl Clone for NodeEvent {
    fn clone(&self) -> Self {
        unimplemented!("Shouldn't be invoked!");
    }
}

/// A [kompact] port for communication
pub struct NodeManagerPort {}
impl Port for NodeManagerPort {
    type Indication = Never;
    type Request = NodeEvent;
}

#[derive(ArconState)]
pub struct NodeManagerState<B: Backend> {
    watermarks: Map<NodeID, Watermark, B>,
    epochs: Map<NodeID, Epoch, B>,
    current_watermark: Value<Watermark, B>,
    current_epoch: Value<Epoch, B>,
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
pub struct NodeManager<B>
where
    B: Backend,
{
    /// Component Context
    ctx: ComponentContext<Self>,
    /// A text description of the operating NodeManager
    ///
    /// e.g., window_sliding_avg_price
    state_id: StateID,
    /// Port for incoming local events from nodes this manager controls
    manager_port: ProvidedPort<NodeManagerPort>,
    /// Port for the SnapshotManager component
    snapshot_manager_port: RequiredPort<SnapshotManagerPort>,
    /// Actor Reference to the EpochManager
    epoch_manager: ActorRefStrong<EpochEvent>,
    /// Current Node parallelism
    node_parallelism: usize,
    /// Max Node parallelism
    max_node_parallelism: usize,
    /// Current Node IDs that are connected to nodes on this manager
    in_channels: Vec<NodeID>,
    /// Monotonically increasing Node ID index
    node_index: u32,
    /// Active Nodes on this NodeManager
    //nodes: FxHashMap<NodeID, Arc<Component<Node<OP, B>>>>,
    /// State Backend used to persist data
    pub backend: Arc<B>,
    /// Internal manager state
    manager_state: NodeManagerState<B>,
}

impl<B> NodeManager<B>
where
    B: Backend,
{
    pub fn new(
        state_id: String,
        epoch_manager: ActorRefStrong<EpochEvent>,
        in_channels: Vec<NodeID>,
        backend: Arc<B>,
    ) -> Self {
        // initialise internal state
        let mut wm_handle = Handle::map("_watermarks");
        let mut epoch_handle = Handle::map("_epochs");
        let mut curr_wm_handle = Handle::value("_curr_watermark");
        let mut curr_epoch_handle = Handle::value("_epoch");

        backend.register_map_handle(&mut wm_handle);
        backend.register_map_handle(&mut epoch_handle);
        backend.register_value_handle(&mut curr_wm_handle);
        backend.register_value_handle(&mut curr_epoch_handle);

        let manager_state = NodeManagerState {
            watermarks: Map::with_capacity(wm_handle.activate(backend.clone()), 64, 64),
            epochs: Map::with_capacity(epoch_handle.activate(backend.clone()), 64, 64),
            current_watermark: Value::new(curr_wm_handle.activate(backend.clone())),
            current_epoch: Value::new(curr_epoch_handle.activate(backend.clone())),
        };

        NodeManager {
            ctx: ComponentContext::uninitialised(),
            state_id,
            manager_port: ProvidedPort::uninitialised(),
            snapshot_manager_port: RequiredPort::uninitialised(),
            epoch_manager,
            node_parallelism: num_cpus::get(),
            max_node_parallelism: (num_cpus::get() * 2) as usize,
            node_index: 0,
            in_channels,
            backend,
            manager_state,
        }
    }

    #[inline]
    fn checkpoint(&mut self) -> ArconResult<()> {
        if let Some(base_dir) = &self.ctx.config()["checkpoint_dir"].as_string() {
            let curr_epoch = self.manager_state.current_epoch().get().unwrap().epoch;

            let checkpoint_dir = format!(
                "{}/checkpoint_{id}_{epoch}",
                base_dir,
                id = self.state_id,
                epoch = curr_epoch,
            );

            self.backend.checkpoint(checkpoint_dir.as_ref())?;

            // Send snapshot to SnapshotManager
            self.snapshot_manager_port.trigger(SnapshotEvent::Snapshot(
                self.state_id.clone(),
                Snapshot::new(curr_epoch, checkpoint_dir.clone()),
            ));

            // Send Ack to EpochManager
            self.epoch_manager.tell(EpochEvent::Ack(
                self.state_id.clone(),
                Epoch::new(curr_epoch),
            ));

            // bump epoch
            self.manager_state.current_epoch().rmw(|e| {
                e.epoch += 1;
            });

            debug!(
                self.ctx.log(),
                "Completed a Checkpoint to path {}", checkpoint_dir
            );
        } else {
            return arcon_err!("Failed to fetch checkpoint_dir from Config");
        }

        Ok(())
    }

    fn handle_node_event(&mut self, event: NodeEvent) -> ArconResult<()> {
        match event {
            NodeEvent::Watermark(id, w) => {
                self.manager_state.watermarks.put(id, w)?;
            }
            NodeEvent::Epoch(id, e) => {
                self.manager_state.epochs.put(id, e)?;
            }
            NodeEvent::Checkpoint(request) => {
                self.checkpoint()?;
                // TODO: actually have to wait for all nodes..
                request.complete();
            }
            #[cfg(feature = "metrics")]
            NodeEvent::Metrics(_, _) => {}
        }
        Ok(())
    }
}

impl<B> ComponentLifecycle for NodeManager<B>
where
    B: Backend,
{
    fn on_start(&mut self) -> Handled {
        info!(self.ctx.log(), "Started NodeManager for {}", self.state_id,);

        // Register state id
        self.snapshot_manager_port
            .trigger(SnapshotEvent::Register(self.state_id.clone()));

        self.epoch_manager
            .tell(EpochEvent::Register(self.state_id.clone()));

        Handled::Ok
    }
}
impl<B> Require<SnapshotManagerPort> for NodeManager<B>
where
    B: Backend,
{
    fn handle(&mut self, _: Never) -> Handled {
        unreachable!("Never can't be instantiated!");
    }
}

impl<B> Provide<SnapshotManagerPort> for NodeManager<B>
where
    B: Backend,
{
    fn handle(&mut self, _: SnapshotEvent) -> Handled {
        Handled::Ok
    }
}

impl<B> Provide<NodeManagerPort> for NodeManager<B>
where
    B: Backend,
{
    fn handle(&mut self, event: NodeEvent) -> Handled {
        if let Err(err) = self.handle_node_event(event) {
            error!(self.ctx.log(), "Failed to handle NodeEvent {:?}", err);
        }

        Handled::Ok
    }
}

impl<B> Require<NodeManagerPort> for NodeManager<B>
where
    B: Backend,
{
    fn handle(&mut self, _: Never) -> Handled {
        unreachable!(crate::data::ArconNever::IS_UNREACHABLE);
    }
}

impl<B> Actor for NodeManager<B>
where
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
