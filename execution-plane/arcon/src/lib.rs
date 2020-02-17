// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

//! Arcon is a Streaming-first Analytics Engine for the Arc language.
//!
//! This crate is not meant to be used directly, but rather relies on
//! [Arc](https://github.com/cda-group/arc) to construct applications and
//! [arcon_codegen] to generate the Rust code.

#![feature(unboxed_closures)]

#[cfg_attr(test, macro_use)]
extern crate arcon_macros;
#[macro_use]
extern crate arcon_error as error;
#[cfg_attr(test, macro_use)]
extern crate abomonation_derive;

/// Arcon data types and serialisers/deserialisers
pub mod data;
/// State backend implementations
pub mod state_backend;
/// Contains the core stream logic
pub mod stream;
/// Utilities for Arcon
pub mod util;

/// Helper module to fetch all macros related to arcon
pub mod macros {
    pub use crate::data::ArconType;
    pub use abomonation_derive::*;
    pub use arcon_macros::*;
}

/// Helper module that imports everything related to arcon into scope
pub mod prelude {
    pub use crate::stream::channel::strategy::{
        broadcast::Broadcast, forward::Forward, key_by::KeyBy, round_robin::RoundRobin,
        ChannelStrategy,
    };
    pub use crate::stream::channel::Channel;
    pub use crate::stream::{
        node::debug::DebugNode,
        node::Node,
        operator::function::{Filter, FlatMap, Map},
        operator::sink::local_file::LocalFileSink,
        operator::window::{AppenderWindow, EventTimeWindowAssigner, IncrementalWindow, Window},
        operator::Operator,
        source::collection::CollectionSource,
        source::local_file::LocalFileSource,
        source::SourceContext,
    };
    #[cfg(feature = "socket")]
    pub use crate::stream::{
        operator::sink::socket::SocketSink,
        source::socket::{SocketKind, SocketSource},
    };

    #[cfg(feature = "kafka")]
    pub use crate::stream::{operator::sink::kafka::KafkaSink, source::kafka::KafkaSource};

    pub use crate::data::flight_serde::{
        reliable_remote::ReliableSerde, unsafe_remote::UnsafeSerde, FlightSerde,
    };
    pub use crate::data::Watermark;
    pub use crate::data::*;
    pub use error::ArconResult;

    pub use kompact::default_components::*;
    pub use kompact::prelude::*;
    #[cfg(feature = "thread_pinning")]
    pub use kompact::{get_core_ids, CoreId};
}

#[cfg(test)]
mod tests {
    use crate::macros::*;
    use std::collections::hash_map::DefaultHasher;

    #[arcon_keyed(id)]
    #[derive(prost::Message)]
    pub struct Item {
        #[prost(uint64, tag = "1")]
        id: u64,
        #[prost(uint32, tag = "2")]
        price: u32,
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
