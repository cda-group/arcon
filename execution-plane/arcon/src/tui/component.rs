// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{manager::node_manager::MetricReport, tui::widgets::node::Node};
use crossbeam_channel::Sender;
use kompact::prelude::*;

/// A simple [kompact] Component that receives metrics from NodeManagers
/// and dispatches the info to the arcon tui.
#[derive(ComponentDefinition)]
pub struct TuiComponent {
    /// Component Context
    ctx: ComponentContext<Self>,
    /// Channel to Arcon Tui Event loop
    sender: Sender<Node>,
}

impl TuiComponent {
    pub fn new(sender: Sender<Node>) -> TuiComponent {
        TuiComponent {
            ctx: ComponentContext::new(),
            sender,
        }
    }
}

impl Provide<ControlPort> for TuiComponent {
    fn handle(&mut self, _: ControlEvent) {}
}

impl Actor for TuiComponent {
    type Message = MetricReport;
    fn receive_local(&mut self, mut msg: Self::Message) {
        let node = Node {
            description: msg.descriptor,
            id: msg.id.id,
            parallelism: msg.parallelism,
            watermark: msg.metrics.watermark.timestamp,
            epoch: msg.metrics.epoch.epoch,
            inbound_channels: msg.metrics.inbound_channels.get() as usize,
            outbound_channels: msg.metrics.outbound_channels.get() as usize,
            inbound_throughput_one_min: msg.metrics.inbound_throughput.get_one_min_rate(),
        };

        self.sender.send(node).unwrap();
    }
    fn receive_network(&mut self, _: NetMessage) {}
}
