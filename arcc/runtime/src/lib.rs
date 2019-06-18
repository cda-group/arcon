extern crate futures;
extern crate tokio;
extern crate weld as weld_core;

#[cfg(feature = "http")]
extern crate http;
#[cfg(feature = "kafka")]
extern crate kafka;

// Public Interface
pub mod error;
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
    pub use slog::*;
    pub use state_backend::*;
}
