// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::nexmark::{
    config::NEXMarkConfig,
    queries::{Query, QueryTimer},
    sink::SinkPort,
    Bid, Event, NEXMarkEvent,
};
use arcon::{pipeline::DynamicNode, prelude::*, state::InMemory, timer::TimerBackend};

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
        state_backend_type: state::BackendType,
    ) -> QueryTimer {
        // Define sink
        let (sink_ref, sink_port_opt) = super::sink::<Bid>(debug_mode, pipeline.system());
        let sink_channel = Channel::Local(sink_ref);
        let forward_to_sink = ChannelStrategy::Forward(Forward::new(
            sink_channel,
            NodeID::new(1),
            pipeline.get_pool_info(),
        ));

        // Define Mapper
        let in_channels = vec![NodeID::new(1)];

        let node_description = String::from("map_node");
        let node_one = q1_node(
            node_description.clone(),
            NodeID::new(0),
            in_channels.clone(),
            forward_to_sink,
            state_backend_type,
        );

        let node_comps =
            pipeline.create_node_manager(node_description, &q1_node, in_channels, vec![node_one]);
        let mapper_ref = node_comps[0].actor_ref().hold().expect("fail");

        start_source(
            nexmark_config,
            pipeline,
            state_backend_type,
            sink_port_opt,
            mapper_ref,
        )
    }
}

pub fn q1_node(
    descriptor: String,
    id: NodeID,
    in_channels: Vec<NodeID>,
    channel_strategy: ChannelStrategy<Bid>,
    state_backend_type: state::BackendType,
) -> DynamicNode<Bid> {
    #[inline(always)]
    fn map_fn(bid: &mut Bid) {
        bid.price = (bid.price * 89) / 100;
    }

    Box::new(Node::new(
        descriptor,
        id,
        in_channels,
        channel_strategy,
        MapInPlace::new(&map_fn),
        InMemory::create("map".as_ref()).unwrap(),
        timer::none(),
    ))
}

fn start_source(
    nexmark_config: NEXMarkConfig,
    pipeline: &mut ArconPipeline,
    state_backend_type: state::BackendType,
    sink_port_opt: Option<ProvidedRef<SinkPort>>,
    mapper_ref: ActorRefStrong<ArconMessage<Bid>>,
) -> QueryTimer {
    let state_backend = state_backend_type;
    let watermark_interval = pipeline.arcon_conf().watermark_interval;
    // Define source context
    let channel_strategy = ChannelStrategy::Forward(Forward::new(
        Channel::Local(mapper_ref),
        NodeID::new(1),
        pipeline.get_pool_info(),
    ));

    let source_context = SourceContext::new(
        watermark_interval,
        None, // no timestamp extractor
        channel_strategy,
        FlatMap::new(&bid_filter_map),
        InMemory::create("src".as_ref()).unwrap(),
        timer::none(),
    );

    super::source(
        sink_port_opt,
        nexmark_config,
        source_context,
        pipeline.system(),
    )
}
