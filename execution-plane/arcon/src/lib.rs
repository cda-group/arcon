#![allow(bare_trait_objects)]
extern crate futures;
extern crate tokio;
extern crate weld as weld_core;

#[cfg_attr(test, macro_use)]
extern crate arcon_macros;
extern crate arcon_messages as messages;
#[macro_use]
extern crate arcon_error as error;
#[cfg_attr(test, macro_use)]
extern crate keyby;
#[cfg_attr(test, macro_use)]
extern crate serde;

pub mod data;
pub mod streaming;
pub mod util;
pub mod weld;

pub mod macros {
    pub use crate::data::ArconType;
    pub use arcon_macros::*;
    pub use keyby::*;
}

pub mod prelude {
    pub use crate::streaming::channel::strategy::{
        broadcast::Broadcast, forward::Forward, key_by::KeyBy, round_robin::RoundRobin,
        shuffle::Shuffle, ChannelStrategy,
    };

    pub use crate::streaming::channel::Channel;
    pub use crate::streaming::task::{filter::Filter, flatmap::FlatMap, map::Map};
    pub use crate::streaming::window::{
        assigner::EventTimeWindowAssigner, builder::WindowBuilder, builder::WindowFn,
        builder::WindowModules,
    };

    pub use crate::streaming::source::{
        collection::CollectionSource, local_file::LocalFileSource, socket::SocketSource,
    };

    pub use crate::streaming::sink::{debug::DebugSink, local_file::LocalFileSink};

    pub use crate::data::{ArconElement, ArconType, ArconVec};
    pub use crate::weld::module::{Module, ModuleRun};
    pub use weld_core::data::*;

    pub use kompact::default_components::*;
    pub use kompact::*;
    pub use slog::*;

    pub use futures::future;
    pub use futures::future::ok;
    pub use futures::prelude::*;

    pub use arcon_messages::protobuf::*;
}

#[cfg(test)]
mod tests {
    use crate::macros::*;
    use std::collections::hash_map::DefaultHasher;

    #[key_by(id)]
    #[arcon]
    pub struct Item {
        id: u64,
        price: u32,
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
