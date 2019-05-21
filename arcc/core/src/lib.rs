extern crate akka_api;
extern crate fnv;
extern crate futures;
extern crate kompact;
extern crate messages;
extern crate regex;
extern crate slog;
extern crate slog_async;
extern crate slog_term;
extern crate state_backend;
extern crate tokio;
extern crate weld as weld_core;

#[cfg(feature = "http")]
extern crate http;
#[cfg(feature = "kafka")]
extern crate kafka;

// Public Interface
pub mod components;
pub mod destination;
pub mod error;
pub mod util;
pub mod weld;

pub mod prelude {
    pub use futures::future;
    pub use futures::future::ok;
    pub use futures::prelude::*;
    pub use kompact::default_components::*;
    pub use kompact::*;
    pub use messages::protobuf::*;
    pub use slog::*;
    pub use state_backend::*;
}
