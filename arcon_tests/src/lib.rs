// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

//! Verifies that Protobuf files can be converted into an Arcon supported format.
//!
//! NOTE: Only Protobuf version 3 is being tested at the moment.
pub mod basic_v3 {
    include!(concat!(env!("OUT_DIR"), "/arcon_basic_v3.rs"));
}
