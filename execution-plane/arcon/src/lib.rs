// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

#![feature(unboxed_closures)]

#[cfg_attr(test, macro_use)]
extern crate arcon_macros;
#[macro_use]
extern crate arcon_error as error;
#[cfg_attr(test, macro_use)]
extern crate abomonation_derive;
#[cfg_attr(test, macro_use)]
extern crate keyby;

pub mod data;
pub mod state_backend;
pub mod stream;
pub mod util;

pub mod macros {
    pub use crate::data::ArconType;
    pub use abomonation_derive::*;
    pub use arcon_macros::*;
    pub use keyby::*;
}

pub mod prelude {
    pub use crate::stream::channel::strategy::{
        broadcast::Broadcast, forward::Forward, key_by::KeyBy, mute::Mute, round_robin::RoundRobin,
        ChannelStrategy,
    };

    pub use crate::stream::channel::Channel;
    pub use crate::stream::{
        node::DebugNode,
        node::Node,
        operator::function::{Filter, FlatMap, Map},
        operator::sink::local_file::LocalFileSink,
        operator::window::{AppenderWindow, EventTimeWindowAssigner, IncrementalWindow, Window},
        operator::Operator,
        source::local_file::LocalFileSource,
    };
    #[cfg(feature = "socket")]
    pub use crate::stream::{
        operator::sink::socket::SocketSink,
        source::socket::{SocketKind, SocketSource},
    };

    #[cfg(feature = "kafka")]
    pub use crate::stream::{operator::sink::kafka::KafkaSink, source::kafka::KafkaSource};

    pub use crate::data::serde::{
        reliable_remote::ReliableSerde, unsafe_remote::UnsafeSerde, ArconSerde,
    };
    pub use crate::data::Watermark;
    pub use crate::data::*;
    pub use error::ArconResult;

    pub use kompact::default_components::*;
    pub use kompact::prelude::*;
    #[cfg(feature = "thread_pinning")]
    pub use kompact::{get_core_ids, CoreId};
    pub use slog::*;
}

#[cfg(test)]
mod tests {
    use crate::macros::*;
    use std::collections::hash_map::DefaultHasher;

    #[key_by(id)]
    #[arcon_decoder(,)]
    #[arcon]
    #[derive(prost::Message)]
    pub struct Item {
        #[prost(uint64, tag = "1")]
        id: u64,
        #[prost(uint32, tag = "2")]
        price: u32,
    }

    #[test]
    fn arcon_decoder_test() {
        use std::str::FromStr;
        let item: Item = Item::from_str("100, 250").unwrap();
        assert_eq!(item.id, 100);
        assert_eq!(item.price, 250);
    }

    #[test]
    fn key_by_macro_test() {
        let i1 = Item { id: 1, price: 20 };
        let i2 = Item { id: 2, price: 150 };
        let i3 = Item { id: 1, price: 50 };

        assert_eq!(calc_hash(&i1), calc_hash(&i3));
        assert_ne!(calc_hash(&i1), calc_hash(&i2));
    }

    fn calc_hash<T: std::hash::Hash>(t: &T) -> u64 {
        use std::hash::Hasher;
        let mut s = DefaultHasher::new();
        t.hash(&mut s);
        s.finish()
    }
}
