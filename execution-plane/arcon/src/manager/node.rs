// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconMessage, Epoch, Watermark},
    prelude::{state, NodeID},
    stream::{
        channel::strategy::ChannelStrategy,
        operator::Operator,
    },
    util::SafelySendableFn,
    ArconType,
};
use crate::manager::state::*;
use arcon_error::*;
use arcon_state::Handle;
use fxhash::FxHashMap;
use kompact::{component::AbstractComponent, prelude::*};
use std::{collections::HashMap, sync::Arc};

pub type CreatedDynamicNode<IN> = Arc<dyn AbstractComponent<Message = ArconMessage<IN>>>;

use crate::stream::node::Node;
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
#[derive(Debug, Clone)]
pub enum NodeEvent {
    #[cfg(feature = "metrics")]
    Metrics(NodeID, NodeMetrics),
    Watermark(NodeID, Watermark),
    Epoch(NodeID, Epoch),
    Checkpoint(NodeID, Epoch),
}

/// A [kompact] port for communication
pub struct NodeManagerPort {}
impl Port for NodeManagerPort {
    type Indication = Never;
    type Request = NodeEvent;
}

use arcon_state::{ArconState, Backend, HashIndex, ValueIndex};

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
pub struct NodeManager<OP, B>
where
    OP: Operator + 'static,
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
    state_manager_port: RequiredPort<StateManagerPort>,
    /// Current Node parallelism
    node_parallelism: usize,
    /// Max Node parallelism
    max_node_parallelism: usize,
    /// Current Node IDs that are connected to nodes on this manager
    in_channels: Vec<NodeID>,
    // Monotonically increasing Node ID index
    //node_index: u32,
    /// Active Nodes on this NodeManager
    nodes: FxHashMap<NodeID, Arc<Component<Node<OP, B>>>>,
    /// State Backend used to persist data
    pub backend: Arc<B>,
    /// Internal manager state
    manager_state: NodeManagerState<B>,
    /// Function to create a Node
    node_fn:
        &'static dyn SafelySendableFn(String, NodeID, ChannelStrategy<OP::OUT>, OP) -> Node<OP, B>,
    checkpoint_requests: HashMap<NodeID, Ask<CheckpointRequest, bool>>,
    active_checkpoint: bool,
}

impl<OP, B> NodeManager<OP, B>
where
    OP: Operator + 'static,
    B: state::Backend,
{
    pub fn new(
        description: String,
        node_fn: &'static dyn SafelySendableFn(
            String,
            NodeID,
            ChannelStrategy<OP::OUT>,
            OP,
        ) -> Node<OP, B>,
        in_channels: Vec<NodeID>,
        backend: Arc<B>,
    ) -> Self {
        let nodes_map: FxHashMap<NodeID, Arc<Component<Node<OP, B>>>> = FxHashMap::default();

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
            watermarks: HashIndex::new(wm_handle.activate(backend.clone()), 32, 32),
            epochs: HashIndex::new(epoch_handle.activate(backend.clone()), 32, 32),
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
            in_channels,
            backend,
            manager_state,
            node_fn,
            nodes: nodes_map,
            checkpoint_requests: HashMap::with_capacity(16),
            active_checkpoint: false,
        }
    }

    #[inline]
    fn checkpoint(&mut self) -> ArconResult<()> {
        if let Some(base_dir) = &self.ctx.config()["checkpoint_dir"].as_string() {
            let checkpoint_dir = format!(
                "{}/checkpoint_{id}_{epoch}",
                base_dir,
                id = self.description,
                epoch = self
                    .manager_state
                    .current_epoch()
                    .get()
                    .ok_or_else(|| arcon_err_kind!("current epoch uninitialized"))?
                    .epoch
            );

            self.backend.checkpoint(checkpoint_dir.as_ref())?;
            //self.state_manager_port.trigger(self.description, epoch, checkpoint_dir)

            debug!(
                self.ctx.log(),
                "Completed a Checkpoint to path {}", checkpoint_dir
            );
        } else {
            return arcon_err!("Failed to fetch checkpoint_dir from Config");
        }

        Ok(())
    }
}

impl<OP, B> ComponentLifecycle for NodeManager<OP, B>
where
    OP: Operator + 'static,
    B: state::Backend,
{
    fn on_start(&mut self) -> Handled {
        info!(
            self.ctx.log(),
            "Started NodeManager for {}", self.description,
        );
        let manager_port = &mut self.manager_port;

        // For each node, connect its NodeManagerPort
        for (node_id, node) in &self.nodes {
            &node.on_definition(|cd| {
                biconnect_ports(manager_port, &mut cd.node_manager_port);
            });
        }
        Handled::Ok
    }
}
impl<OP, B> Require<StateManagerPort> for NodeManager<OP, B>
where
    OP: Operator + 'static,
    B: Backend,
{
    fn handle(&mut self, _: Never) -> Handled {
        unreachable!("Never can't be instantiated!");
    }
}

impl<OP, B> Provide<StateManagerPort> for NodeManager<OP, B>
where
    OP: Operator + 'static,
    B: Backend,
{
    fn handle(&mut self, e: StateEvent) -> Handled {
        trace!(self.log(), "Ignoring node event: {:?}", e);
        Handled::Ok
    }
}

impl<OP, B> Provide<NodeManagerPort> for NodeManager<OP, B>
where
    OP: Operator + 'static,
    B: state::Backend,
{
    fn handle(&mut self, event: NodeEvent) -> Handled {
        debug!(self.ctx.log(), "Got Event {:?}", event);
        Handled::Ok
    }
}

impl<OP, B> Require<NodeManagerPort> for NodeManager<OP, B>
where
    OP: Operator + 'static,
    B: state::Backend,
{
    fn handle(&mut self, _: Never) -> Handled {
        unreachable!(crate::data::ArconNever::IS_UNREACHABLE);
    }
}

impl<OP, B> Actor for NodeManager<OP, B>
where
    OP: Operator + 'static,
    B: state::Backend,
{
    type Message = Ask<CheckpointRequest, bool>;
    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        let request: &CheckpointRequest = msg.request();
        if !self.active_checkpoint {
            // The sender is first node to initialise checkpoint process
            //self.checkpoint_requests.insert(request.0, msg);
            self.checkpoint().unwrap();
            msg.reply(true).expect("reply");
            self.active_checkpoint = false;
            // add Promise to Map and await other nodes if needed.
            // if self.nodes.len() > 1 {
            //   self.checkpoint_requests.put(request.0, request);
            // } else {
            //   self.checkpoint()?;
            //   msg.reply(CheckpointResponse(true);
            // }
        }
        // if Node sent Checkpoint Request, add note of it
        // and check if we still are waiting for more Checkpoint requests
        // from other nodes.
        //
        // If we checkpoint_requests == nodes.len() then
        // self.backend.checkpoint(...);
        // Reply to all Checkpoint request...
        //
        unreachable!();
    }
    fn receive_network(&mut self, _: NetMessage) -> Handled {
        unreachable!();
    }
}



