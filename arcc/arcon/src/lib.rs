#![allow(bare_trait_objects)]
extern crate futures;
extern crate tokio;
extern crate weld as weld_core;

#[cfg_attr(test, macro_use)]
extern crate arcon_macros;
#[cfg_attr(test, macro_use)]
extern crate keyby;
#[cfg_attr(test, macro_use)]
extern crate serde;
#[cfg(feature = "capnproto")]
#[macro_use]
extern crate derivative;

#[macro_use]
pub mod error;
pub mod data;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod messages;
pub mod state_backend;
pub mod streaming;
pub mod util;
pub mod weld;

#[cfg(feature = "capnproto")]
mod messages_capnp {
    include!(concat!(
        env!("OUT_DIR"),
        "/arcon_messages/schema/messages_capnp.rs"
    ));
}

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
    pub use crate::streaming::channel::{Channel, ChannelPort, RequirePortRef};
    pub use crate::streaming::task::stateless::StreamTask;
    pub use crate::streaming::window::{
        assigner::EventTimeWindowAssigner, builder::WindowBuilder, builder::WindowFn,
        builder::WindowModules,
    };

    pub use crate::data::{ArconElement, ArconType, ArconVec};
    pub use crate::weld::module::{Module, ModuleRun};

    pub use kompact::default_components::*;
    pub use kompact::*;
    pub use slog::*;

    pub use futures::future;
    pub use futures::future::ok;
    pub use futures::prelude::*;

    pub use crate::messages::protobuf::*;
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
