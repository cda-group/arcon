// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::nexmark::{
    config::NEXMarkConfig,
    queries::{Query, QueryTimer},
    Auction, Event, NEXMarkEvent, Person,
};
use arcon::{
    pipeline::DynamicNode, prelude::*, state::InMemory, stream::operator::function::FlatMap, timer,
    timer::TimerBackend,
};
use serde::{Deserialize, Serialize};

// SELECT person.name, person.city,
//      person.state, open_auction.id
// FROM open_auction, person, item
// WHERE open_auction.sellerId = person.id
//      AND person.state = ‘OR’
//      AND open_auction.itemid = item.id
//      AND item.categoryId = 10;

#[derive(prost::Oneof, Serialize, Deserialize, Clone, abomonation_derive::Abomonation)]
enum PersonOrAuctionInner {
    #[prost(message, tag = "1")]
    Person(Person),
    #[prost(message, tag = "2")]
    Auction(Auction),
}

#[derive(
    arcon::Arcon, Serialize, Deserialize, prost::Message, Clone, abomonation_derive::Abomonation,
)]
#[arcon(unsafe_ser_id = 500, reliable_ser_id = 501, version = 1)]
pub struct PersonOrAuction {
    #[prost(oneof = "PersonOrAuctionInner", tags = "1, 2")]
    inner: Option<PersonOrAuctionInner>,
}

#[derive(
    arcon::Arcon, Serialize, Deserialize, prost::Message, Clone, abomonation_derive::Abomonation,
)]
#[arcon(unsafe_ser_id = 400, reliable_ser_id = 401, version = 1)]
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

pub struct QueryThree {}

impl Query for QueryThree {
    fn run(
        debug_mode: bool,
        nexmark_config: NEXMarkConfig,
        pipeline: &mut ArconPipeline,
        state_backend_type: state::BackendType,
    ) -> QueryTimer {
        let watermark_interval = pipeline.arcon_conf().watermark_interval;
        let pool_info = pipeline.get_pool_info();
        let mut system = pipeline.system();

        // Define sink
        let (sink_ref, sink_port_opt) = super::sink::<Q3Result>(debug_mode, &mut system);
        let sink_channel = Channel::Local(sink_ref);
        let channel_strategy = ChannelStrategy::Forward(Forward::new(
            sink_channel,
            NodeID::new(1),
            pool_info.clone(),
        ));

        // Define Mapper

        let in_channels = vec![NodeID::new(1)];

        let node_description = String::from("stateful_flatmap_node");
        let node_one = q3_node(
            node_description.clone(),
            NodeID::new(0),
            in_channels.clone(),
            channel_strategy,
            state_backend_type,
        );

        let node_comps =
            pipeline.create_node_manager(node_description, &q3_node, in_channels, vec![node_one]);

        {
            let mut system = pipeline.system();
            // Define source context
            let flatmapper_ref = node_comps.get(0).unwrap().actor_ref().hold().expect("fail");
            let channel = Channel::Local(flatmapper_ref);
            let channel_strategy =
                ChannelStrategy::Forward(Forward::new(channel, NodeID::new(1), pool_info));
            let source_context = SourceContext::new(
                watermark_interval,
                None, // no timestamp extractor
                channel_strategy,
                FlatMap::new(&person_or_auction_filter_map),
                InMemory::create("src".as_ref()).unwrap(),
                timer::none(),
            );

            super::source(sink_port_opt, nexmark_config, source_context, &mut system)
        }
    }
}

state::bundle! {
    struct Q3State {
        person: Handle<ValueState<Person>, u32>,
        pending_auctions: Handle<VecState<Auction>, u32>,
    }
}

impl Q3State {
    fn new() -> Q3State {
        Q3State {
            person: Handle::value("person").with_item_key(0),
            pending_auctions: Handle::vec("pending_auctions").with_item_key(0),
        }
    }
}

pub fn q3_node(
    descriptor: String,
    id: NodeID,
    in_channels: Vec<NodeID>,
    channel_strategy: ChannelStrategy<Q3Result>,
    state_backend_type: state::BackendType,
) -> DynamicNode<PersonOrAuction> {
    // SELECT person.name, person.city,
    //      person.state, open_auction.id
    // FROM open_auction, person, item
    // WHERE open_auction.sellerId = person.id
    //      AND person.state = ‘OR’
    //      AND open_auction.itemid = item.id
    //      AND item.categoryId = 10;

    #[inline(always)]
    fn flatmap_fn(
        person_or_auction: PersonOrAuction,
        state: &Q3State,
        session: &mut state::Session<InMemory>,
    ) -> Vec<Q3Result> {
        let mut state = state.activate(session);

        use PersonOrAuctionInner as P;
        match person_or_auction.inner.unwrap() {
            P::Person(p) => {
                // partitioning
                state.person().set_item_key(p.id);
                state.pending_auctions().set_item_key(p.id);

                state
                    .person()
                    .set(p.clone())
                    .expect("Could not update person state");

                // check if any auctions are pending
                if !state
                    .pending_auctions()
                    .is_empty()
                    .expect("Could not check if pending auctions are empty")
                {
                    let auctions = state
                        .pending_auctions()
                        .get()
                        .expect("Could not get pending auctions");

                    state
                        .pending_auctions()
                        .clear()
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
                state.person().set_item_key(auction.seller);
                state.pending_auctions().set_item_key(auction.seller);

                let person =
                    if let Some(p) = state.person().get().expect("Could not get person state") {
                        p
                    } else {
                        state
                            .pending_auctions()
                            .append(auction)
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

    Box::new(Node::new(
        descriptor,
        id,
        in_channels,
        channel_strategy,
        FlatMap::stateful(Q3State::new(), &flatmap_fn),
        InMemory::create("flatmap".as_ref()).unwrap(),
        timer::none(),
    ))
}
