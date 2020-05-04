// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
pub use snafu::{ensure, ErrorCompat, OptionExt, ResultExt};
use snafu::{Backtrace, Snafu};
use std::result::Result as StdResult;

pub type Result<T, E = ArconStateError> = StdResult<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum ArconStateError {
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
}
