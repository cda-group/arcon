// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
pub use snafu::{ensure, ErrorCompat, OptionExt, ResultExt};
use snafu::{Backtrace, Snafu};
#[cfg(feature = "rocks")]
use std::collections::HashSet;
use std::{io, path::PathBuf, result::Result as StdResult};

pub type Result<T, E = ArconStateError> = StdResult<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum ArconStateError {
    #[snafu(context(false))]
    IO {
        source: io::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Invalid path: {}", path.display()))]
    InvalidPath {
        path: PathBuf,
        backtrace: Backtrace,
    },
    #[snafu(display(
        "Encountered unknown node when trying to restore: {:?}. Known nodes: {:?}",
        unknown_node,
        known_nodes
    ))]
    UnknownNode {
        unknown_node: String,
        known_nodes: Vec<String>,
        backtrace: Backtrace,
    },
    #[snafu(display("Destination buffer is too short: {} < {}", dest_len, needed))]
    FixedBytesSerializationError {
        dest_len: usize,
        needed: usize,
        backtrace: Backtrace,
    },
    #[snafu(display("Source buffer is too short: {} < {}", source_len, needed))]
    FixedBytesDeserializationError {
        source_len: usize,
        needed: usize,
        backtrace: Backtrace,
    },
    #[snafu(context(false))]
    ProtobufDecodeError {
        source: prost::DecodeError,
        backtrace: Backtrace,
    },
    #[snafu(context(false))]
    ProtobufEncodeError {
        source: prost::EncodeError,
        backtrace: Backtrace,
    },
    DummyImplError,
    #[snafu(display("Value in InMemory state backend is of incorrect type"))]
    InMemoryWrongType {
        backtrace: Backtrace,
    },
    #[cfg(feature = "rocks")]
    #[snafu(display("Could not find the requested column family: {:?}", cf_name))]
    RocksMissingColumnFamily {
        cf_name: String,
        backtrace: Backtrace,
    },
    #[cfg(feature = "rocks")]
    #[snafu(display("Could not find options for column family: {:?}", cf_name))]
    RocksMissingOptions {
        cf_name: String,
        backtrace: Backtrace,
    },
    #[cfg(feature = "rocks")]
    #[snafu(context(false))]
    RocksError {
        source: rocksdb::Error,
        backtrace: Backtrace,
    },
    #[cfg(feature = "rocks")]
    #[snafu(display("Rocks state backend is uninitialized! Unknown cfs: {:?}", unknown_cfs))]
    RocksUninitialized {
        backtrace: Backtrace,
        unknown_cfs: HashSet<String>,
    },
    #[cfg(feature = "rocks")]
    #[snafu(display("Rocks restore directory is not empty: {}", dir.display()))]
    RocksRestoreDirNotEmpty {
        backtrace: Backtrace,
        dir: PathBuf,
    },
}
