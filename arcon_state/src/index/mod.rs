// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

pub mod appender;
pub mod map;
pub mod timer;
pub mod value;

use crate::error::Result;

pub use self::{
    appender::{eager::EagerAppender, Appender},
    map::{eager::EagerMap, Map},
    timer::{Timer, TimerEvent},
    value::Value,
};

/// Common Index Operations
///
/// All indexes must implement the IndexOps trait
pub trait IndexOps {
    /// This method ensures all non-persisted data gets pushed to a Backend
    fn persist(&mut self) -> Result<()>;
}

/// Active Arcon State
pub trait ArconState: IndexOps + Send + 'static {}

impl ArconState for () {}
impl IndexOps for () {
    fn persist(&mut self) -> Result<(), crate::error::ArconStateError> {
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn temp_backend() -> crate::Sled {
    use crate::backend::Backend;
    let test_dir = tempfile::tempdir().unwrap();
    let path = test_dir.path();
    crate::backend::Sled::create(path).unwrap()
}
