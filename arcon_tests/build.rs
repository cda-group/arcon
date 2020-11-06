// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

fn main() -> Result<(), Box<dyn std::error::Error>> {
    arcon_build::compile_protos(&["src/basic_v3.proto"], &["src/"]).unwrap();
    Ok(())
}
