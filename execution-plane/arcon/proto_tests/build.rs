// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

extern crate cfg_if;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut basic_v3_cfg = prost_cfg();
    basic_v3_cfg.type_attribute(".", &arcon_attr(104, 105, 1));

    basic_v3_cfg
        .compile_protos(&["src/basic_v3.proto"], &["src/"])
        .unwrap();

    Ok(())
}

fn prost_cfg() -> prost_build::Config {
    let mut config = prost_build::Config::new();
    config.type_attribute(
        ".",
        "#[cfg_attr(feature = \"arcon_serde\", derive(serde::Serialize, serde::Deserialize))]",
    );

    config.type_attribute(
        ".",
        "#[derive(arcon::Arcon, abomonation_derive::Abomonation)]",
    );

    config
}

fn arcon_attr(unsafe_ser_id: usize, reliable_ser_id: usize, version: usize) -> String {
    format!(
        "#[arcon(unsafe_ser_id = {}, reliable_ser_id = {}, version = {})]",
        unsafe_ser_id, reliable_ser_id, version
    )
}
