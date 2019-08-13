#![allow(bare_trait_objects)]
pub extern crate futures;
pub extern crate grpcio;
extern crate protobuf;

// Public Interface
pub mod arconc;
pub mod arconc_grpc;

// TODO: remove?
pub mod prelude {
    pub use futures::sync::*;
    pub use futures::*;
    pub use grpcio::*;
}
