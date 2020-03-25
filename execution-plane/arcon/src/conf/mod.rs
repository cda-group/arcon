// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

/// Configuration for an Arcon Pipeline
#[derive(Clone, Debug)]
pub struct ArconConf {
    /// Base directory for checkpoints
    checkpoint_dir: String,
}

impl Default for ArconConf {
    fn default() -> Self {
        ArconConf {
            checkpoint_dir: String::from("/tmp/arcon"),
        }
    }
}
