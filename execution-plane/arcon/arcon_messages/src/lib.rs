#![allow(bare_trait_objects)]

#[cfg(feature = "proto")]
pub mod protobuf;

#[cfg(feature = "capnproto")]
pub mod capnproto;

#[cfg(feature = "capnproto")]
mod messages_capnp {
    include!(concat!(
        env!("OUT_DIR"),
        "/arcon_messages/schema/messages_capnp.rs"
    ));
}
