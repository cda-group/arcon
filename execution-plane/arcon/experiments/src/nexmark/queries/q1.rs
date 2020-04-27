// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::nexmark::{
    config::NEXMarkConfig,
    queries::{Query, QueryTimer},
    Bid, Event, NEXMarkEvent,
};
use arcon::prelude::*;

// Filter out events that are bids using a FilterMap operator
#[inline(always)]
fn bid_filter_map(mut event: NEXMarkEvent) -> Option<Bid> {
    match event.inner() {
        Event::Bid(bid) => Some(bid),
        _ => None,
    }
}

pub struct QueryOne {}

impl Query for QueryOne {
    /// ConcurrencyConversion
    /// Stream of Bids: Convert bid price from U.S dollars to Euros
    ///
    /// Source(FilterMap -> Bid) -> MapInPlace -> Sink
    fn run(
        debug_mode: bool,
        nexmark_config: NEXMarkConfig,
        pipeline: &mut ArconPipeline,
    ) -> QueryTimer {
        let watermark_interval = pipeline.arcon_conf().watermark_interval;
        let pool_info = pipeline.get_pool_info();
        let mut system = pipeline.system();

        // Define sink
        let (sink_ref, sink_port_opt) = super::sink::<Bid>(debug_mode, &mut system);
        let sink_channel = Channel::Local(sink_ref);
        let channel_strategy = ChannelStrategy::Forward(Forward::new(
            sink_channel,
            NodeID::new(1),
            pool_info.clone(),
        ));

        // Define Mapper

        let in_channels = vec![NodeID::new(1)];

        let node_description = String::from("map_node");
        let node_one = q1_node(
            node_description.clone(),
            NodeID::new(0),
            in_channels.clone(),
            channel_strategy,
        );

        let node_comps =
            pipeline.create_node_manager(node_description, &q1_node, in_channels, vec![node_one]);

        {
            let mut system = pipeline.system();
            // Define source context
            let mapper_ref = node_comps.get(0).unwrap().actor_ref().hold().expect("fail");
            let channel = Channel::Local(mapper_ref);
            let channel_strategy =
                ChannelStrategy::Forward(Forward::new(channel, NodeID::new(1), pool_info.clone()));
            let source_context = SourceContext::new(
                watermark_interval,
                None, // no timestamp extractor
                channel_strategy,
                FilterMap::<NEXMarkEvent, Bid>::new(&bid_filter_map),
                Box::new(InMemory::new("src".as_ref()).unwrap()),
                timer::none,
            );

            super::source(sink_port_opt, nexmark_config, source_context, &mut system)
        }
    }
}

pub fn q1_node(
    descriptor: String,
    id: NodeID,
    in_channels: Vec<NodeID>,
    channel_strategy: ChannelStrategy<Bid>,
) -> Node<impl Operator<IN = Bid, OUT = Bid>> {
    #[inline(always)]
    fn map_fn(bid: &mut Bid) {
        bid.price = (bid.price * 89) / 100;
    }

    Node::new(
        descriptor,
        id,
        in_channels,
        channel_strategy,
        MapInPlace::new(&map_fn),
        Box::new(InMemory::new("map".as_ref()).unwrap()),
        timer::none,
    )
}
