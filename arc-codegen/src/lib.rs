extern crate slog;
extern crate slog_async;
extern crate slog_term;

extern crate fnv;
extern crate futures;
#[cfg(feature = "http")]
extern crate http;
extern crate kompact;
extern crate tokio;
extern crate tokio_threadpool;

extern crate akka_api;
extern crate backend;
extern crate core;

// Public interface
pub mod components;
pub mod error;

pub mod prelude {
    pub use crate::components::*;
    pub use futures::future;
    pub use futures::future::ok;
    pub use futures::prelude::*;
    pub use kompact::*;
    pub use slog::*;
}
