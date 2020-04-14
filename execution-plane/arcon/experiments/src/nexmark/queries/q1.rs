// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::nexmark::{config::NEXMarkConfig, source::NEXMarkSource, Bid, Event, NEXMarkEvent};
use arcon::prelude::*;

// Filter out events that are bids using a FilterMap operator
#[inline(always)]
fn bid_filter_map(mut event: NEXMarkEvent) -> Option<Bid> {
    match event.inner() {
        Event::Bid(bid) => Some(bid),
        _ => None,
    }
}

/// ConcurrencyConversion
/// Stream of Bids: Convert bid price from U.S dollars to Euros
///
/// Source(FilterMap -> Bid) -> MapInPlace -> Sink
pub fn q1(debug_mode: bool, nexmark_config: NEXMarkConfig, pipeline: &mut ArconPipeline) {
    let channel_batch_size = pipeline.arcon_conf().channel_batch_size;
    let watermark_interval = pipeline.arcon_conf().watermark_interval;
    let system = pipeline.system();
    let timeout = std::time::Duration::from_millis(500);

    // If debug mode is enabled, we send the data to a DebugNode.
    // Otherwise, just discard the elements using a Mute strategy...
    let channel_strategy = {
        if debug_mode {
            let sink = system.create(move || DebugNode::<Bid>::new());

            system
                .start_notify(&sink)
                .wait_timeout(timeout)
                .expect("sink never started!");

            let sink_ref: ActorRefStrong<ArconMessage<Bid>> = sink.actor_ref().hold().expect("no");
            let sink_channel = Channel::Local(sink_ref);

            let channel_strategy = ChannelStrategy::Forward(Forward::with_batch_size(
                sink_channel,
                NodeID::new(1),
                channel_batch_size,
            ));

            channel_strategy
        } else {
            ChannelStrategy::Mute
        }
    };

    // Define Mapper

    let in_channels = vec![NodeID::new(1)];

    let node_description = String::from("map_node");
    let node_one = q1_node(
        node_description.clone(),
        NodeID::new(0),
        in_channels.clone(),
        channel_strategy.clone(),
    );

    let node_comps = pipeline.create_node_manager(
        node_description,
        &q1_node,
        in_channels,
        channel_strategy,
        vec![node_one],
    );

    {
        let system = pipeline.system();
        // Define source context
        let mapper_ref = node_comps.get(0).unwrap().actor_ref().hold().expect("fail");
        let channel = Channel::Local(mapper_ref);
        let channel_strategy = ChannelStrategy::Forward(Forward::new(channel, NodeID::new(1)));
        let source_context = SourceContext::new(
            watermark_interval,
            None, // no timestamp extractor
            channel_strategy,
            Box::new(FilterMap::<NEXMarkEvent, Bid>::new(&bid_filter_map)),
            Box::new(InMemory::new("src".as_ref()).unwrap()),
        );

        let nexmark_source_comp = system
            .create_dedicated(move || NEXMarkSource::<Bid>::new(nexmark_config, source_context));

        system.start(&nexmark_source_comp);
    }
}

pub fn q1_node(
    descriptor: String,
    id: NodeID,
    in_channels: Vec<NodeID>,
    channel_strategy: ChannelStrategy<Bid>,
) -> Node<Bid, Bid> {
    #[inline(always)]
    fn map_fn(bid: &mut Bid) {
        bid.price = (bid.price * 89) / 100;
    }

    Node::new(
        descriptor,
        id,
        in_channels,
        channel_strategy,
        Box::new(MapInPlace::new(&map_fn)),
        Box::new(InMemory::new("map".as_ref()).unwrap()),
    )
}
