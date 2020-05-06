// Copyright (c) 2017, 2018 ETH Zurich
// SPDX-License-Identifier: MIT

// Modifications Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

pub mod config;
pub mod queries;
pub mod sink;
pub mod source;

use arcon::prelude::*;
use config::NEXMarkConfig;
use rand::{rngs::SmallRng, seq::SliceRandom, Rng};
use serde::{Deserialize, Serialize};
use std::cmp::{max, min};

trait NEXMarkRNG {
    fn gen_string(&mut self, m: usize) -> String;
    fn gen_price(&mut self) -> usize;
}

impl NEXMarkRNG for SmallRng {
    fn gen_string(&mut self, _m: usize) -> String {
        String::new()
    }
    fn gen_price(&mut self) -> usize {
        (10.0_f32.powf(self.gen::<f32>() * 6.0) * 100.0).round() as usize
    }
}

#[derive(
    arcon::Arcon,
    serde::Serialize,
    serde::Deserialize,
    prost::Message,
    Clone,
    abomonation_derive::Abomonation,
)]
#[arcon(unsafe_ser_id = 104, reliable_ser_id = 105, version = 1)]
pub struct Person {
    #[prost(uint32, tag = "1")]
    pub id: u32,
    #[prost(string, tag = "2")]
    pub name: String,
    #[prost(string, tag = "3")]
    pub email_address: String,
    #[prost(string, tag = "4")]
    pub credit_card: String,
    #[prost(string, tag = "5")]
    pub city: String,
    #[prost(string, tag = "6")]
    pub state: String,
    #[prost(uint32, tag = "7")]
    pub date: u32,
}

impl Person {
    #[inline]
    pub fn new(id: u32, time: u32, mut rng: &mut SmallRng, nex: &NEXMarkConfig) -> Person {
        Person {
            id,
            name: String::new(),
            email_address: String::new(),
            credit_card: String::new(),
            city: nex.us_cities.choose(&mut rng).unwrap().clone(),
            state: nex.us_states.choose(&mut rng).unwrap().clone(),
            date: time,
        }
    }
    #[inline]
    fn next_id(id: u32, rng: &mut SmallRng, nex: &NEXMarkConfig) -> u32 {
        let people = Self::last_id(id, nex) + 1;
        let active = min(people, nex.num_active_people as u32);
        people - active + rng.gen_range(0, active + nex.person_id_lead as u32)
    }
    #[inline]
    fn last_id(id: u32, nex: &NEXMarkConfig) -> u32 {
        let epoch = id / nex.proportion_denominator;
        let mut offset = id % nex.proportion_denominator;
        if nex.person_proportion <= offset {
            offset = nex.person_proportion - 1;
        }
        epoch * nex.person_proportion + offset
    }
}

#[derive(
    arcon::Arcon,
    serde::Serialize,
    serde::Deserialize,
    prost::Message,
    Clone,
    abomonation_derive::Abomonation,
)]
#[arcon(unsafe_ser_id = 106, reliable_ser_id = 107, version = 1)]
pub struct Auction {
    #[prost(uint32, tag = "1")]
    pub id: u32,
    #[prost(string, tag = "2")]
    pub item_name: String,
    #[prost(string, tag = "3")]
    pub description: String,
    #[prost(uint32, tag = "4")]
    pub initial_bid: u32,
    #[prost(uint32, tag = "5")]
    pub reserve: u32,
    #[prost(uint32, tag = "6")]
    pub date: u32,
    #[prost(uint32, tag = "7")]
    pub expires: u32,
    #[prost(uint32, tag = "8")]
    pub seller: u32,
    #[prost(uint32, tag = "9")]
    pub category: u32,
}

impl Auction {
    #[inline]
    pub fn new(
        events_so_far: u32,
        id: u32,
        time: u32,
        rng: &mut SmallRng,
        nex: &NEXMarkConfig,
    ) -> Self {
        let initial_bid = rng.gen_price() as u32;
        let seller = if rng.gen_range(0, nex.hot_seller_ratio) > 0 {
            (Person::last_id(id, nex) / nex.hot_seller_ratio_2) * nex.hot_seller_ratio_2 as u32
        } else {
            Person::next_id(id, rng, nex)
        };
        Auction {
            id: Self::last_id(id, nex) + nex.first_auction_id as u32,
            item_name: rng.gen_string(20),
            description: rng.gen_string(100),
            initial_bid,
            reserve: initial_bid + rng.gen_price() as u32,
            date: time,
            expires: time + Self::next_length(events_so_far, rng, time, nex),
            seller: seller + nex.first_person_id,
            category: nex.first_category_id + rng.gen_range(0, nex.num_categories),
        }
    }

    #[inline]
    fn next_id(id: u32, rng: &mut SmallRng, nex: &NEXMarkConfig) -> u32 {
        let max_auction = Self::last_id(id, nex);
        let min_auction = if max_auction < nex.in_flight_auctions {
            0
        } else {
            max_auction - nex.in_flight_auctions
        };
        min_auction
            + rng.gen_range(
                0,
                max_auction - min_auction + 1 + nex.auction_id_lead as u32,
            )
    }

    #[inline]
    fn last_id(id: u32, nex: &NEXMarkConfig) -> u32 {
        let mut epoch = id / nex.proportion_denominator;
        let mut offset = id % nex.proportion_denominator;
        if offset < nex.person_proportion {
            epoch -= 1;
            offset = nex.auction_proportion - 1;
        } else if nex.person_proportion + nex.auction_proportion <= offset {
            offset = nex.auction_proportion - 1;
        } else {
            offset -= nex.person_proportion;
        }
        epoch * nex.auction_proportion + offset
    }

    #[inline]
    fn next_length(events_so_far: u32, rng: &mut SmallRng, time: u32, nex: &NEXMarkConfig) -> u32 {
        let current_event = nex.next_adjusted_event(events_so_far);
        let events_for_auctions =
            (nex.in_flight_auctions * nex.proportion_denominator) / nex.auction_proportion;
        let future_auction = nex.event_timestamp_ns(current_event + events_for_auctions);
        let horizon = future_auction - time;
        1 + rng.gen_range(0, max(horizon * 2, 1))
    }
}

#[derive(
    arcon::Arcon,
    serde::Serialize,
    serde::Deserialize,
    prost::Message,
    Clone,
    abomonation_derive::Abomonation,
)]
#[arcon(unsafe_ser_id = 108, reliable_ser_id = 109, version = 1)]
pub struct Bid {
    #[prost(uint32, tag = "1")]
    pub auction: u32,
    #[prost(uint32, tag = "2")]
    pub bidder: u32,
    #[prost(uint32, tag = "3")]
    pub price: u32,
    #[prost(uint32, tag = "4")]
    pub date: u32,
}

impl Bid {
    #[inline]
    fn new(id: u32, time: u32, rng: &mut SmallRng, nex: &NEXMarkConfig) -> Self {
        let auction = if 0 < rng.gen_range(0, nex.hot_auction_ratio as usize) {
            (Auction::last_id(id, nex) / nex.hot_auction_ratio_2) * nex.hot_auction_ratio_2
        } else {
            Auction::next_id(id, rng, nex)
        };
        let bidder = if 0 < rng.gen_range(0, nex.hot_bidder_ratio) {
            (Person::last_id(id, nex) / nex.hot_bidder_ratio_2) * nex.hot_bidder_ratio_2 + 1
        } else {
            Person::next_id(id, rng, nex)
        };
        Bid {
            auction: auction + nex.first_auction_id,
            bidder: bidder + nex.first_person_id,
            price: rng.gen_price() as u32,
            date: time,
        }
    }
}

#[derive(
    arcon::Arcon,
    serde::Serialize,
    serde::Deserialize,
    prost::Message,
    Clone,
    abomonation_derive::Abomonation,
)]
#[arcon(unsafe_ser_id = 600, reliable_ser_id = 601, version = 1)]
pub struct NEXMarkEvent {
    #[prost(oneof = "Event", tags = "1, 2, 3")]
    inner: Option<Event>,
}

impl NEXMarkEvent {
    #[inline]
    pub fn create(events_so_far: u32, rng: &mut SmallRng, nex: &mut NEXMarkConfig) -> Self {
        let rem = nex.next_adjusted_event(events_so_far) % nex.proportion_denominator;
        let timestamp = nex.event_timestamp_ns(nex.next_adjusted_event(events_so_far));
        let id = nex.first_event_id + nex.next_adjusted_event(events_so_far);

        if rem < nex.person_proportion {
            NEXMarkEvent {
                inner: Some(Event::Person(Person::new(id, timestamp, rng, nex))),
            }
        } else if rem < nex.person_proportion + nex.auction_proportion {
            NEXMarkEvent {
                inner: Some(Event::Auction(Auction::new(
                    events_so_far,
                    id,
                    timestamp,
                    rng,
                    nex,
                ))),
            }
        } else {
            NEXMarkEvent {
                inner: Some(Event::Bid(Bid::new(id, timestamp, rng, nex))),
            }
        }
    }

    #[inline]
    pub fn id(&self) -> u32 {
        match *self.inner.as_ref().unwrap() {
            Event::Person(ref p) => p.id,
            Event::Auction(ref a) => a.id,
            Event::Bid(ref b) => b.auction, // Bid events don't have ids, so use the associated auction id
        }
    }
    #[inline]
    pub fn time(&self) -> u32 {
        match *self.inner.as_ref().unwrap() {
            Event::Person(ref p) => p.date,
            Event::Auction(ref a) => a.date,
            Event::Bid(ref b) => b.date,
        }
    }

    #[inline]
    pub fn inner(&mut self) -> Event {
        self.inner.take().unwrap()
    }
}

#[derive(prost::Oneof, Serialize, Deserialize, Clone, abomonation_derive::Abomonation)]
pub enum Event {
    #[prost(message, tag = "1")]
    Person(Person),
    #[prost(message, tag = "2")]
    Auction(Auction),
    #[prost(message, tag = "3")]
    Bid(Bid),
}

impl Event {
    #[inline]
    pub fn is_bid(&self) -> bool {
        match self {
            Event::Bid(_) => true,
            _ => false,
        }
    }
    #[inline]
    pub fn is_person(&self) -> bool {
        match self {
            Event::Person(_) => true,
            _ => false,
        }
    }
    #[inline]
    pub fn is_auction(&self) -> bool {
        match self {
            Event::Auction(_) => true,
            _ => false,
        }
    }
}
