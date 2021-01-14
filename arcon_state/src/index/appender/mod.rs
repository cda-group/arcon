// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    backend::{
        handles::{ActiveHandle, Handle},
        Backend, VecState,
    },
    data::Value,
    error::*,
    index::IndexOps,
};
use std::sync::Arc;

const DEFAULT_APPENDER_SIZE: usize = 1024;

pub mod eager;

/// An Index suitable for Non-associative Windows
///
/// A backing [VecState] acts as an overflow vector when
/// the data no longer fits in the specified in-memory capacity.
#[derive(Debug)]
pub struct Appender<V, B>
where
    V: Value,
    B: Backend,
{
    /// In-memory Vector of elements
    elements: Vec<V>,
    /// A handle to the VecState
    handle: ActiveHandle<B, VecState<V>>,
}

impl<V, B> Appender<V, B>
where
    V: Value,
    B: Backend,
{
    /// Creates an Appender using the default appender size
    pub fn new(id: impl Into<String>, backend: Arc<B>) -> Self {
        let mut handle = Handle::vec(id.into());
        backend.register_vec_handle(&mut handle);
        let handle = handle.activate(backend);

        Appender {
            elements: Vec::with_capacity(DEFAULT_APPENDER_SIZE),
            handle,
        }
    }

    /// Creates an Appender with specified capacity
    pub fn with_capacity(id: impl Into<String>, capacity: usize, backend: Arc<B>) -> Self {
        let mut handle = Handle::vec(id.into());
        backend.register_vec_handle(&mut handle);
        let handle = handle.activate(backend);

        Appender {
            elements: Vec::with_capacity(capacity),
            handle,
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        let mem_len = self.elements.len();
        let raw_len = self.handle.len().unwrap();
        mem_len + raw_len
    }

    /// Clear the vector of elements and its backing overflow [VecState]
    #[inline(always)]
    pub fn clear(&mut self) -> Result<()> {
        self.elements.clear();
        self.handle.clear()
    }

    /// Consume the whole batch of data
    #[inline(always)]
    pub fn consume(&mut self) -> Result<Vec<V>> {
        // get elements stored in VecState
        let mut stored = self.handle.get()?;

        // swap vec with the current one..
        let mut new_vec: Vec<V> = Vec::with_capacity(self.elements.capacity());
        std::mem::swap(&mut new_vec, &mut self.elements);
        // append the current in-mem elements with the stored ones
        stored.append(&mut new_vec);

        // make sure we clear the backing VecState
        self.handle.clear()?;

        Ok(stored)
    }

    #[inline(always)]
    pub fn append(&mut self, data: V) -> Result<()> {
        if self.elements.len() == self.elements.capacity() {
            self.persist()?;
        }
        self.elements.push(data);
        Ok(())
    }
}

impl<V, B> IndexOps for Appender<V, B>
where
    V: Value,
    B: Backend,
{
    fn persist(&mut self) -> Result<()> {
        self.handle.add_all(self.elements.drain(..))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn appender_test() {
        let backend = Arc::new(crate::backend::temp_backend());
        let mut index = Appender::new("appender", backend);

        for i in 0..1024 {
            index.append(i as u64).unwrap();
        }
        assert_eq!(index.len(), 1024);
        let consumed = index.consume().unwrap();
        assert_eq!(consumed.len(), 1024);
        for (c, i) in consumed.into_iter().enumerate() {
            assert_eq!(c as u64, i);
        }
        index.clear().unwrap();
        let consumed = index.consume().unwrap();
        assert_eq!(consumed.is_empty(), true);
    }
}
