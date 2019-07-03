#![allow(warnings)]
#![feature(dyn_trait)]
extern crate futures;
extern crate tokio;
extern crate weld as weld_core;
#[macro_use]
extern crate keyby;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate macros;

#[cfg(feature = "http")]
extern crate http;
#[cfg(feature = "kafka")]
extern crate kafka;

#[macro_use]
pub mod error;
pub mod data;
pub mod streaming;
pub mod util;
pub mod weld;

pub mod prelude {
    pub use futures::future;
    pub use futures::future::ok;
    pub use futures::prelude::*;
    pub use kompact::default_components::*;
    pub use kompact::*;
    pub use messages::protobuf::*;
    pub use serde::de::DeserializeOwned;
    pub use serde::{Deserialize, Serialize};
    pub use slog::*;
    pub use state_backend::*;

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;
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
