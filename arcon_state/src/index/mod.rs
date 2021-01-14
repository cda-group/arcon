// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

pub mod appender;
pub mod hash_table;
pub mod timer;
pub mod value;

use crate::error::Result;

pub use self::{
    appender::{eager::EagerAppender, Appender},
    hash_table::{eager::EagerHashTable, HashTable},
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
pub trait ArconState: IndexOps + Send + 'static {
    const STATE_ID: &'static str;
}

/// Identifier for empty ArconState ()
pub const EMPTY_STATE_ID: &str = "!";

impl ArconState for () {
    const STATE_ID: &'static str = EMPTY_STATE_ID;
}
impl IndexOps for () {
    fn persist(&mut self) -> Result<(), crate::error::ArconStateError> {
        Ok(())
    }
}
