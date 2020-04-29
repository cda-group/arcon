// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::NodeID,
    stream::{
        channel::strategy::ChannelStrategy,
        node::{Node, NodeMetrics},
        operator::Operator,
    },
    util::SafelySendableFn,
};
use fxhash::FxHashMap;
use kompact::prelude::*;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct MetricReport {
    pub(crate) descriptor: String,
    pub(crate) id: NodeID,
    pub(crate) parallelism: usize,
    pub(crate) metrics: NodeMetrics,
}

/// Enum containing possible local node events
#[derive(Debug, Clone)]
pub enum NodeEvent {
    Metrics(NodeID, NodeMetrics),
}

/// A [kompact] port for communication
pub struct NodeManagerPort {}
impl Port for NodeManagerPort {
    type Indication = Never;
    type Request = NodeEvent;
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
pub struct NodeManager<OP>
where
    OP: Operator + 'static,
{
    /// Component Context
    ctx: ComponentContext<Self>,
    /// A text description of the operating NodeManager
    ///
    /// e.g., window_sliding_avg_price
    node_description: String,
    /// Port for incoming local events
    manager_port: ProvidedPort<NodeManagerPort, Self>,
    /// Current Node parallelism
    node_parallelism: usize,
    /// Max Node parallelism
    max_node_parallelism: usize,
    /// Current Node IDs that are connected to nodes on this manager
    in_channels: Vec<NodeID>,
    /// Monotonically increasing Node ID index
    node_index: u32,
    /// Nodes this manager controls
    nodes: FxHashMap<NodeID, Arc<Component<Node<OP>>>>,
    /// Metrics per Node
    node_metrics: FxHashMap<NodeID, NodeMetrics>,
    /// Port reference to the previous NodeManager in the pipeline stage
    ///
    /// It is defined as an Option as source components won't have any prev_manager
    prev_manager: Option<RequiredRef<NodeManagerPort>>,
    /// Port reference to the next NodeManager in the pipeline stage
    ///
    /// It is defined as an Option as sink components won't have any next_manager
    next_manager: Option<RequiredRef<NodeManagerPort>>,
    /// Function to create a Node on this NodeManager
    node_fn: &'static dyn SafelySendableFn(
        String,
        NodeID,
        Vec<NodeID>,
        ChannelStrategy<OP::OUT>,
    ) -> Node<OP>,
    #[cfg(feature = "arcon_tui")]
    tui_ref: ActorRefStrong<MetricReport>,
}

impl<OP> NodeManager<OP>
where
    OP: Operator + 'static,
{
    pub fn new(
        node_description: String,
        node_fn: &'static dyn SafelySendableFn(
            String,
            NodeID,
            Vec<NodeID>,
            ChannelStrategy<OP::OUT>,
        ) -> Node<OP>,
        in_channels: Vec<NodeID>,
        node_comps: Vec<Arc<Component<Node<OP>>>>,
        prev_manager: Option<RequiredRef<NodeManagerPort>>,
        next_manager: Option<RequiredRef<NodeManagerPort>>,
        #[cfg(feature = "arcon_tui")] tui_ref: ActorRefStrong<MetricReport>,
    ) -> NodeManager<OP> {
        let total_nodes = node_comps.len() as u32;
        let mut nodes_map = FxHashMap::default();
        for (i, node) in node_comps.into_iter().enumerate() {
            let node_id = NodeID::new(i as u32);
            nodes_map.insert(node_id, node);
        }
        NodeManager {
            ctx: ComponentContext::new(),
            node_description,
            manager_port: ProvidedPort::new(),
            node_parallelism: total_nodes as usize,
            max_node_parallelism: (num_cpus::get() * 2) as usize,
            in_channels,
            nodes: nodes_map,
            node_metrics: FxHashMap::default(),
            node_index: total_nodes,
            prev_manager,
            next_manager,
            node_fn,
            #[cfg(feature = "arcon_tui")]
            tui_ref,
        }
    }
}

impl<OP> Provide<ControlPort> for NodeManager<OP>
where
    OP: Operator + 'static,
{
    fn handle(&mut self, event: ControlEvent) {
        match event {
            ControlEvent::Start => {
                info!(
                    self.ctx.log(),
                    "Started NodeManager for {}", self.node_description
                );

                let manager_port = &mut self.manager_port;
                // For each node, connect its NodeManagerPort to NodeManager
                for (_, node) in &self.nodes {
                    &node.on_definition(|cd| {
                        let p = &mut cd.node_manager_port;
                        biconnect_ports::<NodeManagerPort, _, _>(manager_port, p);
                    });
                }
            }
            ControlEvent::Kill => {}
            ControlEvent::Stop => {}
        }
    }
}

impl<OP> Provide<NodeManagerPort> for NodeManager<OP>
where
    OP: Operator + 'static,
{
    fn handle(&mut self, event: NodeEvent) {
        debug!(self.ctx.log(), "Got Event {:?}", event);
        match event {
            NodeEvent::Metrics(id, metrics) => {
                #[cfg(feature = "arcon_tui")]
                {
                    let report = MetricReport {
                        descriptor: self.node_description.clone(),
                        id,
                        parallelism: self.node_parallelism,
                        metrics: metrics.clone(),
                    };
                    self.tui_ref.tell(report);
                }

                self.node_metrics.insert(id, metrics);
            }
        }
    }
}

impl<OP> Require<NodeManagerPort> for NodeManager<OP>
where
    OP: Operator + 'static,
{
    fn handle(&mut self, _: Never) {
        unreachable!(crate::data::ArconNever::IS_UNREACHABLE);
    }
}

impl<OP> Actor for NodeManager<OP>
where
    OP: Operator + 'static,
{
    type Message = NodeEvent;
    fn receive_local(&mut self, _: Self::Message) {}
    fn receive_network(&mut self, _: NetMessage) {}
}
