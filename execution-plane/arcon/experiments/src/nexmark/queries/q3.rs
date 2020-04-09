// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::nexmark::{
    config::NEXMarkConfig, source::NEXMarkSource, Auction, Event, NEXMarkEvent, Person,
};
use arcon::{
    macros::*,
    prelude::*,
    stream::operator::{function::StatefulFlatMap, OperatorContext},
};
use serde::{Deserialize, Serialize};

// SELECT person.name, person.city,
//      person.state, open_auction.id
// FROM open_auction, person, item
// WHERE open_auction.sellerId = person.id
//      AND person.state = ‘OR’
//      AND open_auction.itemid = item.id
//      AND item.categoryId = 10;

#[derive(prost::Oneof, Serialize, Deserialize, Clone, Abomonation, Hash)]
enum PersonOrAuctionInner {
    #[prost(message, tag = "1")]
    Person(Person),
    #[prost(message, tag = "2")]
    Auction(Auction),
}

#[arcon]
pub struct PersonOrAuction {
    #[prost(oneof = "PersonOrAuctionInner", tags = "1, 2")]
    inner: Option<PersonOrAuctionInner>,
}

#[arcon]
pub struct Q3Result {
    #[prost(string, tag = "1")]
    seller_name: String,
    #[prost(string, tag = "2")]
    seller_city: String,
    #[prost(string, tag = "3")]
    seller_state: String,
    #[prost(uint32, tag = "4")]
    auction_id: u32,
}

#[inline(always)]
fn person_or_auction_filter_map(mut event: NEXMarkEvent) -> Option<PersonOrAuction> {
    use PersonOrAuctionInner as P;
    match event.inner() {
        Event::Person(person) if &person.state == "OR" => Some(PersonOrAuction {
            inner: Some(P::Person(person)),
        }),
        Event::Auction(auction) => Some(PersonOrAuction {
            inner: Some(P::Auction(auction)),
        }),
        _ => None,
    }
}

pub fn q3(debug_mode: bool, nexmark_config: NEXMarkConfig, pipeline: &mut ArconPipeline) {
    let channel_batch_size = pipeline.arcon_conf().channel_batch_size;
    let watermark_interval = pipeline.arcon_conf().watermark_interval;
    let system = pipeline.system();
    let timeout = std::time::Duration::from_millis(500);

    // If debug mode is enabled, we send the data to a DebugNode.
    // Otherwise, just discard the elements using a Mute strategy...
    let channel_strategy = {
        if debug_mode {
            let sink = system.create(move || DebugNode::<Q3Result>::new());

            system
                .start_notify(&sink)
                .wait_timeout(timeout)
                .expect("sink never started!");

            let sink_ref: ActorRefStrong<ArconMessage<Q3Result>> =
                sink.actor_ref().hold().expect("no");
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

    let node_description = String::from("stateful_flatmap_node");
    let node_one = q3_node(
        node_description.clone(),
        NodeID::new(0),
        in_channels.clone(),
        channel_strategy.clone(),
    );

    let node_comps = pipeline.create_node_manager(
        node_description,
        &q3_node,
        in_channels,
        channel_strategy,
        vec![node_one],
    );

    {
        let system = pipeline.system();
        // Define source context
        let flatmapper_ref = node_comps.get(0).unwrap().actor_ref().hold().expect("fail");
        let channel = Channel::Local(flatmapper_ref);
        let channel_strategy = ChannelStrategy::Forward(Forward::new(channel, NodeID::new(1)));
        let source_context = SourceContext::new(
            watermark_interval,
            None, // no timestamp extractor
            channel_strategy,
            Box::new(FilterMap::<NEXMarkEvent, PersonOrAuction>::new(
                &person_or_auction_filter_map,
            )),
            Box::new(InMemory::new("src").unwrap()),
        );

        let nexmark_source_comp =
            system.create_dedicated(move || NEXMarkSource::new(nexmark_config, source_context));

        system.start(&nexmark_source_comp);
    }
}

pub fn q3_node(
    descriptor: String,
    id: NodeID,
    in_channels: Vec<NodeID>,
    channel_strategy: ChannelStrategy<Q3Result>,
) -> Node<PersonOrAuction, Q3Result> {
    // SELECT person.name, person.city,
    //      person.state, open_auction.id
    // FROM open_auction, person, item
    // WHERE open_auction.sellerId = person.id
    //      AND person.state = ‘OR’
    //      AND open_auction.itemid = item.id
    //      AND item.categoryId = 10;

    #[inline(always)]
    fn flatmap_fn(
        ctx: OperatorContext<Q3Result>,
        person_or_auction: PersonOrAuction,
    ) -> Vec<Q3Result> {
        const PERSON: &str = "person_state";
        const PENDING_AUCTIONS: &str = "pending_auctions";

        use PersonOrAuctionInner as P;
        match person_or_auction.inner.unwrap() {
            P::Person(p) => {
                let person_state = ctx
                    .state_backend
                    .build(PERSON)
                    .with_item_key(p.id) // partitioning
                    .value::<Person>();

                person_state
                    .set(ctx.state_backend, p.clone())
                    .expect("Could not update person state");

                // check if any auctions are pending
                let pending_auctions = ctx
                    .state_backend
                    .build(PENDING_AUCTIONS)
                    .with_item_key(p.id)
                    .vec::<Auction>();

                if !pending_auctions
                    .is_empty(ctx.state_backend)
                    .expect("Could not check if pending auctions are empty")
                {
                    let auctions = pending_auctions
                        .get(ctx.state_backend)
                        .expect("Could not get pending auctions");

                    pending_auctions
                        .clear(ctx.state_backend)
                        .expect("Could not clear pending auctions");

                    auctions
                        .into_iter()
                        .flat_map(|a| {
                            if a.category == 10 {
                                Some(Q3Result {
                                    seller_name: p.name.clone(),
                                    seller_city: p.city.clone(),
                                    seller_state: p.state.clone(),
                                    auction_id: p.id,
                                })
                            } else {
                                None
                            }
                        })
                        .collect()
                } else {
                    vec![]
                }
            }
            P::Auction(auction) => {
                let person_state = ctx
                    .state_backend
                    .build(PERSON)
                    .with_item_key(auction.seller) // partitioning
                    .value::<Person>();

                let person = if let Some(p) = person_state
                    .get(ctx.state_backend)
                    .expect("Could not get person state")
                {
                    p
                } else {
                    // we don't have a user with that id, so add to pending
                    let pending_auctions = ctx
                        .state_backend
                        .build(PENDING_AUCTIONS)
                        .with_item_key(auction.seller)
                        .vec::<Auction>();

                    pending_auctions
                        .append(ctx.state_backend, auction)
                        .expect("Could not store the auction");

                    return vec![];
                };

                if auction.category == 10 {
                    vec![Q3Result {
                        seller_name: person.name,
                        seller_city: person.city,
                        seller_state: person.state,
                        auction_id: auction.id,
                    }]
                } else {
                    vec![]
                }
            }
        }
    }

    Node::new(
        descriptor,
        id,
        in_channels,
        channel_strategy,
        Box::new(StatefulFlatMap::new(&flatmap_fn)),
        Box::new(InMemory::new("flatmap").unwrap()),
    )
}
