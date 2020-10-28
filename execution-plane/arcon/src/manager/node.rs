// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{Epoch, Watermark},
    manager::state::*,
    prelude::{state, NodeID},
};
use arcon_error::*;
use arcon_state::{ArconState, Backend, Handle, HashIndex, ValueIndex};
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

#[derive(Debug, Clone)]
pub struct CheckpointRequest(pub NodeID, pub Epoch);
#[derive(Debug, Clone)]
pub struct CheckpointResponse(pub bool);

/// Enum containing possible local node events
#[derive(Debug)]
pub enum NodeEvent {
    #[cfg(feature = "metrics")]
    Metrics(NodeID, NodeMetrics),
    Watermark(NodeID, Watermark),
    Epoch(NodeID, Epoch),
    //Checkpoint(NodeID, Epoch, KPromise<()>),
    Checkpoint(NodeID, Epoch),
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
    watermarks: HashIndex<NodeID, Watermark, B>,
    epochs: HashIndex<NodeID, Epoch, B>,
    current_watermark: ValueIndex<Watermark, B>,
    current_epoch: ValueIndex<Epoch, B>,
}

/// A [kompact] component responsible for coordinating a set of Arcon nodes
///
/// The following illustrates the role of a NodeManager in the context of an ArconPipeline
///
/// ```text
///                 ArconPipeline
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
    B: state::Backend,
{
    /// Component Context
    ctx: ComponentContext<Self>,
    /// A text description of the operating NodeManager
    ///
    /// e.g., window_sliding_avg_price
    description: String,
    /// Port for incoming local events from nodes this manager controls
    manager_port: ProvidedPort<NodeManagerPort>,
    /// Port for the StateManager component
    state_manager_port: RequiredPort<StateManagerPort>,
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
    B: state::Backend,
{
    pub fn new(description: String, in_channels: Vec<NodeID>, backend: Arc<B>) -> Self {
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
            watermarks: HashIndex::with_capacity(wm_handle.activate(backend.clone()), 64, 64),
            epochs: HashIndex::with_capacity(epoch_handle.activate(backend.clone()), 64, 64),
            current_watermark: ValueIndex::new(curr_wm_handle.activate(backend.clone())),
            current_epoch: ValueIndex::new(curr_epoch_handle.activate(backend.clone())),
        };

        NodeManager {
            ctx: ComponentContext::uninitialised(),
            description,
            manager_port: ProvidedPort::uninitialised(),
            state_manager_port: RequiredPort::uninitialised(),
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
                id = self.description,
                epoch = curr_epoch,
            );

            self.backend.checkpoint(checkpoint_dir.as_ref())?;

            // Checkpoint complete, send update to the StateManager
            self.state_manager_port.trigger(StateEvent::Snapshot(
                self.description.clone(),
                Snapshot::new(curr_epoch, checkpoint_dir.clone()),
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
            NodeEvent::Checkpoint(_, _) => {
                self.checkpoint()?;
            }
        }
        Ok(())
    }
}

impl<B> ComponentLifecycle for NodeManager<B>
where
    B: state::Backend,
{
    fn on_start(&mut self) -> Handled {
        info!(
            self.ctx.log(),
            "Started NodeManager for {}", self.description,
        );
        /*
        let manager_port = &mut self.manager_port;

        // For each node, connect its NodeManagerPort
        for (node_id, node) in &self.nodes {
            &node.on_definition(|cd| {
                biconnect_ports(manager_port, &mut cd.node_manager_port);
            });
        }
        */
        Handled::Ok
    }
}
impl<B> Require<StateManagerPort> for NodeManager<B>
where
    B: Backend,
{
    fn handle(&mut self, _: Never) -> Handled {
        unreachable!("Never can't be instantiated!");
    }
}

impl<B> Provide<StateManagerPort> for NodeManager<B>
where
    B: Backend,
{
    fn handle(&mut self, _: StateEvent) -> Handled {
        Handled::Ok
    }
}

impl<B> Provide<NodeManagerPort> for NodeManager<B>
where
    B: state::Backend,
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
    B: state::Backend,
{
    fn handle(&mut self, _: Never) -> Handled {
        unreachable!(crate::data::ArconNever::IS_UNREACHABLE);
    }
}

impl<B> Actor for NodeManager<B>
where
    B: state::Backend,
{
    type Message = Never;
    fn receive_local(&mut self, _: Self::Message) -> Handled {
        Handled::Ok
    }
    fn receive_network(&mut self, _: NetMessage) -> Handled {
        unreachable!();
    }
}
