// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    nexmark::{Bid, Event, NEXMarkEvent},
    throughput_sink::ThroughputSink,
};
use arcon::prelude::*;

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
pub fn q1(pipeline: &mut ArconPipeline) {
    let channel_batch_size = pipeline.arcon_conf().channel_batch_size;
    let system = pipeline.system();
    let timeout = std::time::Duration::from_millis(500);

    // Define Sink
    let sink = system.create(move || ThroughputSink::<Bid>::new(1, true, 100));

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

    // Define Mapper

    let node_description = String::from("map_node");
    let node_one = q1_node(
        node_description.clone(),
        NodeID::new(0),
        vec![NodeID::new(1)],
        channel_strategy.clone(),
    );
    let node_two = q1_node(
        node_description.clone(),
        NodeID::new(1),
        vec![NodeID::new(1)],
        channel_strategy.clone(),
    );

    //let map_comp = system.create(|| node_one);
    pipeline.create_node_manager(
        node_description,
        &q1_node,
        vec![NodeID::new(1)],
        channel_strategy,
        vec![node_one, node_two],
    );

    /*
    system
        .start_notify(&map_comp)
        .wait_timeout(timeout)
        .expect("mapper never started!");

    let mapper_ref: ActorRefStrong<ArconMessage<Bid>> = map_comp.actor_ref().hold().expect("no");
    */

    // Create node manager
    /*
     */

    //mapper_ref
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
        Box::new(InMemory::new("perf").unwrap()),
    )
}
