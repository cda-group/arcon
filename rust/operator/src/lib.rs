#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

extern crate kompact;
extern crate futures;
extern crate tokio;
extern crate tokio_threadpool;
#[cfg(feature = "http")]
extern crate http;

extern crate backend;
extern crate akka_api;
extern crate core;

// Public interface
pub mod error;
pub mod io;
pub mod components;

mod util;
//mod akka;

pub mod prelude {
    pub use crate::io::*;
    pub use futures::future;
    pub use futures::future::ok;
    pub use futures::prelude::*;
    pub use kompact::*;
    pub use slog::*;
}
