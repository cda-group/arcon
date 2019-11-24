#[cfg(feature = "arconc")]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/mod.rs"));
}

#[cfg(feature = "arconc")]
pub use proto::arconc::*;
#[cfg(feature = "arconc")]
pub use proto::arconc_grpc::*;

#[cfg(feature = "arcon_spec")]
pub mod arcon_spec {
    include!(concat!(env!("OUT_DIR"), "/arcon_spec.rs"));
}
