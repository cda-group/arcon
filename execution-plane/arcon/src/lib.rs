#![allow(bare_trait_objects)]

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
pub mod streaming;
pub mod util;

pub mod macros {
    pub use crate::data::ArconType;
    pub use arcon_macros::*;
    pub use keyby::*;
    pub use abomonation_derive::*;
}

pub mod prelude {
    pub use crate::streaming::channel::strategy::{
        broadcast::Broadcast, forward::Forward, key_by::KeyBy, round_robin::RoundRobin,
        shuffle::Shuffle, ChannelStrategy,
    };

    pub use crate::streaming::channel::Channel;
    pub use crate::streaming::node::Node;
    pub use crate::streaming::operator::{Filter, Map, Operator};
    pub use crate::streaming::window::{
        event_time::EventTimeWindowAssigner, AppenderWindow, IncrementalWindow, Window,
    };

    pub use crate::streaming::source::local_file::LocalFileSource;

    #[cfg(feature = "socket")]
    pub use crate::streaming::{
        sink::socket::SocketSink,
        source::socket::{SocketKind, SocketSource},
    };

    pub use crate::streaming::sink::{debug::DebugSink, local_file::LocalFileSink};

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
        #[prost(uint64, tag="1")]
        id: u64,
        #[prost(uint32, tag="2")]
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
        assert!(calc_hash(&i1) != calc_hash(&i2));
    }

    fn calc_hash<T: std::hash::Hash>(t: &T) -> u64 {
        use std::hash::Hasher;
        let mut s = DefaultHasher::new();
        t.hash(&mut s);
        s.finish()
    }
}
