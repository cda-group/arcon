// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

//! `arcon_build` is a wrapper around `prost-build` that adds the required attributes for it to work in arcon.
//!
//! ```toml
//! [dependencies]
//! arcon = <arcon-version>
//! serde = <arcon-serde-version>
//! prost = <arcon-prost-version>
//! abomonation = <arcon-abomonation-version>
//! abomonation_derive = <arcon-abomonation-derive-version>
//!
//! [build-dependencies]
//! arcon_build = { version = <arcon-version> }
//! ```
//!
//! ## Example .proto file
//!
//! ```proto
//! syntax = "proto3";
//!
//! package arcon_data;
//!
//! // unsafe_ser_id = 100
//! // reliable_ser_id = 101
//! // version = 1
//! message Hello {
//! string id = 1
//! }
//! ```
//!
//! Generate the Rust code by creating a `build.rs` build script and use the
//! `compile_protos` function:
//!
//! ```rust,no_run
//! # use std::io::Result;
//! fn main() -> Result<()> {
//!   arcon_build::compile_protos(&["src/path_to_file.proto"], &["src/"])?;
//!   Ok(())
//! }
//! ```

use std::{io::Result, path::Path};

/// Compile Protobuf files with Arcon configured attributes
pub fn compile_protos<P>(protos: &[P], includes: &[P]) -> Result<()>
where
    P: AsRef<Path>,
{
    let mut config = prost_build::Config::new();
    config.type_attribute(
        ".",
        "#[cfg_attr(feature = \"arcon_serde\", derive(serde::Serialize, serde::Deserialize))]",
    );

    config.type_attribute(
        ".",
        "#[derive(arcon::Arcon, abomonation_derive::Abomonation)]",
    );

    config.compile_protos(protos, includes)
}
