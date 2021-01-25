// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

pub mod appender;
pub mod hash_table;
pub mod timer;
pub mod value;

use crate::{
    data::{Key, Value},
    error::Result,
};
use std::borrow::Cow;

pub use self::{
    appender::{eager::EagerAppender, Appender},
    hash_table::{eager::EagerHashTable, HashTable},
    timer::{Timer, TimerEvent},
    value::{EagerValue, LocalValue},
};

/// Common Index Operations
///
/// All indexes must implement the IndexOps trait
pub trait IndexOps {
    /// This method ensures all non-persisted data gets pushed to a Backend
    fn persist(&mut self) -> Result<()>;
    /// Set the current active key for the index
    fn set_key(&mut self, key: u64);
}

/// Active Arcon State
pub trait ArconState: IndexOps + Send + 'static {
    const STATE_ID: &'static str;
}

/// Identifier for empty ArconState ()
pub const EMPTY_STATE_ID: &str = "!";

pub type EmptyState = ();

impl ArconState for () {
    const STATE_ID: &'static str = EMPTY_STATE_ID;
}
impl IndexOps for () {
    fn persist(&mut self) -> Result<(), crate::error::ArconStateError> {
        Ok(())
    }
    fn set_key(&mut self, _: u64) {
        // ignore
    }
}

pub trait AppenderIndex<V>: IndexOps
where
    V: Value,
{
    /// Add data to an Appender
    fn append(&mut self, value: V) -> Result<()>;
    /// Consumes the Appender
    ///
    /// Safety: Note that this call loads the data eagerly and may lead to problems if there is a
    /// lack of system memory.
    fn consume(&mut self) -> Result<Vec<V>>;
    /// Returns the length of the Appender
    fn len(&self) -> usize;
    /// Method to check whether an Appender is empty
    fn is_empty(&self) -> bool;
}

/// Index for Maintaining a single value per Key
///
/// Keys are set by the Arcon runtime.
pub trait ValueIndex<V>: IndexOps
where
    V: Value,
{
    /// Blind update of the current value
    fn put(&mut self, value: V) -> Result<()>;
    /// Fetch the current value. If no value exists, None
    /// will be returned.
    ///
    /// The returned value is wrapped in a [Cow] in order to
    /// support both owned and referenced values depending on
    /// whether the index is Eager or Lazy.
    fn get(&self) -> Result<Option<Cow<V>>>;
    /// Removes value and returns an owned version of the
    /// value if it exists.
    fn remove(&mut self) -> Result<Option<V>>;
    /// Clear value if it exists
    fn clear(&mut self) -> Result<()>;
    /// Read-Modify-Write operation
    ///
    /// If the value does not exist, V::Default will be inserted.
    fn rmw<F>(&mut self, f: F) -> Result<()>
    where
        F: FnMut(&mut V) + Sized;
}

/// Index for Maintaining a Hash Table per Key
pub trait HashIndex<K, V>: IndexOps
where
    K: Key,
    V: Value,
{
    /// Blind insert
    fn put(&mut self, key: &K, value: V) -> Result<()>;
    /// Fetch an Value by Key
    fn get(&self, key: &K) -> Result<Option<V>>;
    /// Attempt to remove value and return it if it exists
    fn remove(&mut self, key: &K) -> Result<Option<V>>;
    /// Clear value by key
    fn clear(&mut self, key: &K) -> Result<()>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn rmw<F>(&mut self, key: &K, value: V)
    where
        F: FnMut(&mut V) + Sized;
}
