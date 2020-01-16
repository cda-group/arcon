use std::error::Error as StdError;
use std::fmt;

/// Helper macro for generating an Arcon Error
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
}

#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    cause: Option<Box<dyn StdError + Send>>,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let ErrorKind::ArconError(ref msg) = self.kind;
        write!(f, "{}", msg)
    }
}

impl StdError for Error {
    fn cause(&self) -> Option<&dyn StdError> {
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

pub type ArconResult<T> = ::std::result::Result<T, crate::Error>;
