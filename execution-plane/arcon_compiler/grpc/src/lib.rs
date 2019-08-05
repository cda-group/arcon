#![allow(bare_trait_objects)]
extern crate futures;
extern crate grpcio;
extern crate protobuf;

// Public Interface
pub mod arconc;
pub mod arconc_grpc;

pub mod prelude {
    pub use futures::sync::*;
    pub use futures::*;
    pub use grpcio::*;
}
