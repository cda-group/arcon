// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

/// AppenderIndex suitable for Non-associative Windows
pub mod appender;
/// HashIndex suitable for point lookups and random reads
pub mod hash;
pub mod timer;
/// ValueIndex suitable for single object operations
pub mod value;

use crate::error::Result;

/// Common Index Operations
///
/// All indexes must implement the IndexOps trait
pub trait IndexOps {
    /// This method ensures all non-persisted data gets pushed to a Backend
    fn persist(&mut self) -> Result<()>;
}
