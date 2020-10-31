// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

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
