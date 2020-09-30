// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    backend::{handles::Handle, Backend, BackendContainer, ValueState},
    data::Value,
    error::*,
    index::IndexOps,
};
use std::sync::Arc;

/// An Index suitable for single value operations
///
/// Examples include rolling counters, watermarks, and epochs.
#[derive(Debug)]
pub struct ValueIndex<V, B>
where
    V: Value,
    B: Backend,
{
    /// The data itself
    data: Option<V>,
    /// Modified flag
    modified: bool,
    /// A handle to the ValueState
    handle: Handle<ValueState<V>>,
    /// Reference to the underlying Backend
    backend: Arc<BackendContainer<B>>,
}

impl<V, B> ValueIndex<V, B>
where
    V: Value,
    B: Backend,
{
    /// Creates a ValueIndex
    pub fn new(key: &'static str, backend: Arc<BackendContainer<B>>) -> Self {
        // register handle
        let mut handle = Handle::value(key);
        handle.register(&mut unsafe {
            crate::backend::RegistrationToken::new(&mut backend.clone().session())
        });

        ValueIndex {
            data: Some(V::default()),
            modified: false,
            handle,
            backend,
        }
    }

    /// Clear the data in the index layer, but also the backing ValueState.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.data = None;
        let mut sb_session = self.backend.session();
        let mut state = self.handle.activate(&mut sb_session);
        let _ = state.clear();
    }

    /// Access the index value through an Option.
    #[inline(always)]
    pub fn get(&self) -> Option<&V> {
        self.data.as_ref()
    }

    /// Blind insert
    ///
    /// Sets the Index data and sets its modify flag to true.
    #[inline(always)]
    pub fn put(&mut self, data: V) {
        self.data = Some(data);
        self.modified = true;
    }

    /// Read-Modify-Write Operation
    ///
    /// If the ValueIndex is set, then the function `F`
    /// is passed a mutable reference to the data. It is then assumed
    /// that the data has been changed, thus the modified flag is set to true.
    #[inline(always)]
    pub fn rmw<F: Sized>(&mut self, mut f: F) -> bool
    where
        F: FnMut(&mut V),
    {
        if let Some(ref mut v) = self.data.as_mut() {
            // execute the modification
            f(v);
            // assume the data has actually been modified
            self.modified = true;
            // indicate that the rmw has successfully modified the data
            return true;
        }
        // Failed to modify ValueIndex
        return false;
    }
}

impl<V, B> IndexOps for ValueIndex<V, B>
where
    V: Value,
    B: Backend,
{
    fn persist(&mut self) -> Result<()> {
        if let Some(data) = &self.data {
            // only push data to the handle if it has actually been modified
            if self.modified {
                let mut sb_session = self.backend.session();
                let mut state = self.handle.activate(&mut sb_session);
                state.fast_set_by_ref(data)?;
                self.modified = false;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::in_memory::InMemory;

    #[test]
    fn value_index_test() {
        let backend = InMemory::create(&std::path::Path::new("/tmp/")).unwrap();
        let mut index: ValueIndex<u64, InMemory> =
            ValueIndex::new("_valueindex", std::sync::Arc::new(backend));
        assert_eq!(index.get(), Some(&0u64));
        index.put(10u64);
        assert_eq!(index.get(), Some(&10u64));
        index.rmw(|v| {
            *v += 10;
        });
        assert_eq!(index.get(), Some(&20u64));
    }
}
