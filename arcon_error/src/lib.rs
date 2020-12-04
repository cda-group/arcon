// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

//! The arcon_error crate provide error utilities for Arcon related crates.

use arcon_state::error::ArconStateError;
use std::{error::Error as StdError, fmt};

/// Helper macro for generating an Error
#[macro_export]
macro_rules! arcon_err {
    ( $($arg:tt)* ) => ({
        ::std::result::Result::Err($crate::Error::new_arcon(format!($($arg)*)))
    })
}

/// Helper macro for generating ErrorKind
#[macro_export]
macro_rules! arcon_err_kind {
    ( $($arg:tt)* ) => ({
        $crate::Error::new_arcon(format!($($arg)*))
    })
}

#[derive(Debug)]
pub enum ErrorKind {
    ArconError(String),
    StateError(arcon_state::error::ArconStateError),
}

#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    cause: Option<Box<dyn StdError + Send>>,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.kind {
            ErrorKind::ArconError(msg) => write!(f, "{}", msg),
            ErrorKind::StateError(e) => write!(f, "{}", e),
        }
    }
}

impl StdError for Error {
    fn cause(&self) -> Option<&dyn StdError> {
        if let ErrorKind::StateError(e) = &self.kind {
            return e.source();
        }

        match self.cause {
            Some(ref x) => Some(&**x),
            None => None,
        }
    }
}

impl Error {
    pub fn new(kind: ErrorKind) -> Self {
        Self { kind, cause: None }
    }
    pub fn new_arcon(err_msg: String) -> Self {
        Self {
            kind: ErrorKind::ArconError(err_msg),
            cause: None,
        }
    }
    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }
}

impl From<ArconStateError> for Error {
    fn from(e: ArconStateError) -> Self {
        Error {
            kind: ErrorKind::StateError(e),
            cause: None,
        }
    }
}

/// A Result type for Arcon related crates
pub type ArconResult<T> = ::std::result::Result<T, crate::Error>;
/// A Result type used by Arcon Operators
pub type OperatorResult<T> = ::std::result::Result<T, ArconStateError>;

pub trait ResultExt<T> {
    fn ctx(self, message: &str) -> ArconResult<T>;
}

impl<T, E> ResultExt<T> for Result<T, E>
where
    E: StdError + Send + 'static,
{
    fn ctx(self, message: &str) -> ArconResult<T> {
        self.map_err(|cause| {
            let mut err = arcon_err_kind!("{}", message);
            err.cause = Some(Box::new(cause));
            err
        })
    }
}
