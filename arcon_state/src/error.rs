pub use snafu::{ensure, ErrorCompat, OptionExt, ResultExt};
use snafu::{Backtrace, Snafu};
#[cfg(feature = "rocks")]
use std::collections::HashSet;
#[cfg(all(feature = "faster", target_os = "linux"))]
use std::sync::mpsc::RecvTimeoutError;
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
    InvalidPath { path: PathBuf, backtrace: Backtrace },
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

    #[snafu(display("Value in InMemory state backend is of incorrect type"))]
    InMemoryWrongType { backtrace: Backtrace },

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
    RocksRestoreDirNotEmpty { backtrace: Backtrace, dir: PathBuf },

    #[cfg(all(feature = "faster", target_os = "linux"))]
    #[snafu(display("Faster did not send the result in time"))]
    FasterReceiveTimeout {
        source: RecvTimeoutError,
        backtrace: Backtrace,
    },
    #[cfg(all(feature = "faster", target_os = "linux"))]
    #[snafu(display(
        "Faster call returned an unexpected status: {} ({})",
        faster_format(status),
        status
    ))]
    FasterUnexpectedStatus { backtrace: Backtrace, status: u8 },
    #[cfg(all(feature = "faster", target_os = "linux"))]
    #[snafu(context(false))]
    FasterOtherError {
        #[snafu(source(from(faster_rs::FasterError<'_>, faster_error_make_static)))]
        source: faster_rs::FasterError<'static>,
        backtrace: Backtrace,
    },
    #[cfg(all(feature = "faster", target_os = "linux"))]
    #[snafu(display("Faster checkpoint failed"))]
    FasterCheckpointFailed { backtrace: Backtrace },
    #[cfg(all(feature = "faster", target_os = "linux"))]
    #[snafu(display("Error in faster rmw: {}", message))]
    FasterErrorInRmw {
        message: String,
        backtrace: Backtrace,
    },

    #[cfg(feature = "sled")]
    #[snafu(context(false))]
    SledError {
        source: ::sled::Error,
        backtrace: Backtrace,
    },
    #[snafu(display("Error : {}", msg))]
    Unknown { msg: String },
}

#[cfg(all(feature = "faster", target_os = "linux"))]
fn faster_format(status: &u8) -> &'static str {
    match *status {
        0 => "OK",
        1 => "PENDING",
        2 => "NOT_FOUND",
        3 => "OUT_OF_MEMORY",
        4 => "IO_ERROR",
        5 => "CORRUPTION",
        6 => "ABORTED",
        _ => "?",
    }
}

#[cfg(all(feature = "faster", target_os = "linux"))]
fn faster_error_make_static(err: faster_rs::FasterError) -> faster_rs::FasterError<'static> {
    // so... this is a bummer. Every FasterError ever created actually is 'static, but for some
    // reason the lifetime param is there. The lifetime is only associated with the BuilderError
    // variant, and that's constructed only in one place, with a static str literal
    unsafe { std::mem::transmute(err) }
}
