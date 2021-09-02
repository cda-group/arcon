//! Verifies that Protobuf files can be converted into an Arcon supported format.
//!
//! NOTE: Only Protobuf version 3 is being tested at the moment.
pub mod basic_v3 {
    include!(concat!(env!("OUT_DIR"), "/arcon_basic_v3.rs"));
}

/// Tests for deriving `prost` from structs.
mod proto_derive_test;
