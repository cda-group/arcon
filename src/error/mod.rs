// Copyright (c) 2021, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

/// A helper macro to indicate a reportable bug
#[macro_export]
macro_rules! reportable_error {
    ( $($arg:tt)* ) => ({
        $crate::error::ArconResult::Err($crate::error::Error::ReportableBug { msg: format!($($arg)*) })
    })
}

pub mod timer;

use arcon_state::error::ArconStateError;
use snafu::{Backtrace, Snafu};
use std::io;

// Inspired by Sled's error management approach.
// http://sled.rs/errors.html

/// Top level Result type in Arcon
pub type ArconResult<T> = std::result::Result<T, Error>;
/// Alias for State Error
pub type StateResult<T> = std::result::Result<T, ArconStateError>;

/// A top level Error type holding Arcon pipeline errors that cause a total system halt
#[derive(Debug, Snafu)]
pub enum Error {
    /// The system has been used in an unsupported way.
    #[snafu(display("Unsupported operation {}", msg))]
    Unsupported { msg: String },
    /// An unexpected bug has happened. Please open an issue on github!
    #[snafu(display(
        "Unexpected bug {} please report at https://github.com/cda-group/arcon",
        msg
    ))]
    ReportableBug { msg: String },
    /// A read or write error has happened when interacting with the file system.
    #[snafu(display("An IO error occured {}", error))]
    Io { error: io::Error },
    /// An error that indicates possible data corruption
    ///
    /// It could for example be that serialisation keeps failing.
    #[snafu(display("Unexpected data corruption {} with backtrace {:?}", msg, backtrace))]
    Corruption { msg: String, backtrace: Backtrace },
}

impl From<io::Error> for Error {
    #[inline]
    fn from(io_error: io::Error) -> Self {
        Error::Io { error: io_error }
    }
}

impl From<Error> for io::Error {
    fn from(error: Error) -> io::Error {
        use self::Error::*;
        use std::io::ErrorKind;
        match error {
            Io { error } => error,
            Unsupported { ref msg } => io::Error::new(
                ErrorKind::InvalidInput,
                format!("operation not supported: {:?}", msg),
            ),
            ReportableBug { ref msg } => io::Error::new(
                ErrorKind::Other,
                format!(
                    "unexpected bug! please report this bug at github.com/cda-group/arcon {:?}",
                    msg
                ),
            ),
            Corruption { msg, .. } => io::Error::new(
                ErrorKind::InvalidData,
                format!("corruption encountered: {:?}", msg),
            ),
        }
    }
}

// Transform ArconStateError into top level Error
impl From<ArconStateError> for Error {
    fn from(error: ArconStateError) -> Self {
        let msg = error.to_string();
        match error {
            ArconStateError::IO { source, .. } => Error::Io { error: source },
            ArconStateError::FixedBytesSerializationError { backtrace, .. } => {
                Error::Corruption { msg, backtrace }
            }
            ArconStateError::FixedBytesDeserializationError { backtrace, .. } => {
                Error::Corruption { msg, backtrace }
            }
            ArconStateError::ProtobufDecodeError { backtrace, .. } => {
                Error::Corruption { msg, backtrace }
            }
            ArconStateError::ProtobufEncodeError { backtrace, .. } => {
                Error::Corruption { msg, backtrace }
            }
            #[cfg(feature = "rocksdb")]
            ArconStateError::RocksError { .. } => Error::ReportableBug { msg },
            ArconStateError::SledError { .. } => Error::ReportableBug { msg },
            // Transform rest of errors as unsupported
            _ => Error::Unsupported { msg },
        }
    }
}
