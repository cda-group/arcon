// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    backend::{handles::ActiveHandle, Backend, ValueState},
    error::*,
    index::IndexOps,
};

/// An Index suitable for single value operations
///
/// Examples include rolling counters, watermarks, and epochs.
#[derive(Debug)]
pub struct Value<V, B>
where
    V: crate::data::Value,
    B: Backend,
{
    /// The data itself
    data: Option<V>,
    /// Modified flag
    modified: bool,
    /// A handle to the ValueState
    handle: ActiveHandle<B, ValueState<V>>,
}

impl<V, B> Value<V, B>
where
    V: crate::data::Value,
    B: Backend,
{
    /// Creates a Value
    pub fn new(handle: ActiveHandle<B, ValueState<V>>) -> Self {
        // Attempt to fetch data from backend, otherwise set to default value..
        let data = match handle.get() {
            Ok(Some(v)) => v,
            Ok(None) => V::default(),
            Err(_) => V::default(),
        };

        Value {
            data: Some(data),
            modified: false,
            handle,
        }
    }

    /// Clear the data in the index layer, but also the backing ValueState.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.data = None;
        let _ = self.handle.clear();
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
    /// If the Value is set, then the function `F`
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
        // Failed to modify Value
        false
    }
}

impl<V, B> IndexOps for Value<V, B>
where
    V: crate::data::Value,
    B: Backend,
{
    fn persist(&mut self) -> Result<()> {
        if let Some(data) = &self.data {
            // only push data to the handle if it has actually been modified
            if self.modified {
                self.handle.fast_set_by_ref(data)?;
                self.modified = false;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{sled::Sled, Handle};
    use std::sync::Arc;

    #[test]
    fn value_index_test() {
        let backend = Sled::create(&std::path::Path::new("/tmp/value")).unwrap();
        let backend = Arc::new(backend);
        let mut handle = Handle::value("_value");
        backend.register_value_handle(&mut handle);
        let active = handle.activate(backend);
        let mut index: Value<u64, Sled> = Value::new(active);
        assert_eq!(index.get(), Some(&0u64));
        index.put(10u64);
        assert_eq!(index.get(), Some(&10u64));
        index.rmw(|v| {
            *v += 10;
        });
        assert_eq!(index.get(), Some(&20u64));
    }
}
