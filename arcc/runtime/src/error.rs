use std::error::Error as StdError;
use std::fmt;

#[macro_export]
macro_rules! arcon_err {
    ( $($arg:tt)* ) => ({
        ::std::result::Result::Err($crate::error::Error::new_arcon(format!($($arg)*)))
    })
}

/// Internal helper macro for generating ErrorKin
macro_rules! arcon_err_kind {
    ( $($arg:tt)* ) => ({
        $crate::error::Error::new_arcon(format!($($arg)*))
    })
}

/// Internal helper macro for defining Weld errors
macro_rules! weld_error {
    ( $($arg:tt)* ) => ({
        $crate::error::Error::new_weld(format!($($arg)*))
    })
}

#[derive(Debug)]
pub enum ErrorKind {
    WeldError(String),
    ArconError(String),
}

#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    cause: Option<Box<StdError + Send>>,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let msg = match self.kind {
            ErrorKind::ArconError(ref err) => err,
            ErrorKind::WeldError(ref err) => err,
        };
        write!(f, "{}", msg)
    }
}

impl StdError for Error {
    fn cause(&self) -> Option<&StdError> {
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
    pub fn new_weld(err_msg: String) -> Self {
        Self {
            kind: ErrorKind::WeldError(err_msg),
            cause: None,
        }
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

pub type ArconResult<T> = ::std::result::Result<T, crate::error::Error>;
