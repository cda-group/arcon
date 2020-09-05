// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    error::*, handles::Handle, hint::unlikely, index::IndexOps, Backend, BackendContainer, Value,
    VecState,
};
use std::rc::Rc;

/// An Index suitable for Non-associative Windows
///
/// A backing [VecState] acts as an overflow vector when
/// the data no longer fits in the specified in-memory capacity.
#[derive(Debug)]
pub struct AppenderIndex<V, B>
where
    V: Value,
    B: Backend,
{
    /// In-memory Vector of elements
    elements: Vec<V>,
    /// A handle to the VecState
    handle: Handle<VecState<V>>,
    /// Reference to the underlying Backend
    backend: Rc<BackendContainer<B>>,
}

impl<V, B> AppenderIndex<V, B>
where
    V: Value,
    B: Backend,
{
    /// Creates an AppenderIndex
    pub fn new(key: &'static str, capacity: usize, backend: Rc<BackendContainer<B>>) -> Self {
        // register handle
        let mut handle = Handle::vec(key);
        handle.register(&mut unsafe {
            crate::RegistrationToken::new(&mut backend.clone().session())
        });

        AppenderIndex {
            elements: Vec::with_capacity(capacity),
            handle,
            backend,
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        let mem_len = self.elements.len();
        let mut sb_session = self.backend.session();
        let state = self.handle.activate(&mut sb_session);
        let raw_len = state.len().unwrap();
        mem_len + raw_len
    }

    /// Clear the vector of elements and its backing overflow [VecState]
    #[inline(always)]
    pub fn clear(&mut self) -> Result<()> {
        self.elements.clear();

        let mut sb_session = self.backend.session();
        let mut state = self.handle.activate(&mut sb_session);
        state.clear()
    }

    /// Consume the whole AppenderIndex
    ///
    /// This function will clear the data afterwards.
    #[inline(always)]
    pub fn consume(&mut self) -> Result<Vec<V>> {
        let mut sb_session = self.backend.session();
        let mut state = self.handle.activate(&mut sb_session);

        // get elements stored in VecState
        let mut stored = state.get()?;

        // swap vec with the current one..
        let mut new_vec: Vec<V> = Vec::with_capacity(self.elements.capacity());
        std::mem::swap(&mut new_vec, &mut self.elements);
        // append the current in-mem elements with the stored ones
        stored.append(&mut new_vec);

        // make sure we clear the backing VecState
        state.clear()?;

        Ok(stored)
    }

    #[inline(always)]
    pub fn append(&mut self, data: V) -> Result<()> {
        if unlikely(self.elements.len() == self.elements.capacity()) {
            self.persist()?;
        }
        self.elements.push(data);
        Ok(())
    }
}

impl<V, B> IndexOps for AppenderIndex<V, B>
where
    V: Value,
    B: Backend,
{
    fn persist(&mut self) -> Result<()> {
        let mut sb_session = self.backend.session();
        let mut state = self.handle.activate(&mut sb_session);
        state.add_all(self.elements.drain(..))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn appender_test() {
        use crate::in_memory::InMemory;
        let backend = crate::InMemory::create(&std::path::Path::new("/tmp/")).unwrap();
        let capacity = 524;
        let mut index: AppenderIndex<u64, InMemory> =
            AppenderIndex::new("_appender", capacity, std::rc::Rc::new(backend));

        for i in 0..1024 {
            index.append(i as u64).unwrap();
        }
        assert_eq!(index.len(), 1024);
        let consumed = index.consume().unwrap();
        assert_eq!(consumed.len(), 1024);
        let mut c = 0;
        for i in consumed {
            assert_eq!(c, i);
            c += 1;
        }
        index.clear().unwrap();
        let consumed = index.consume().unwrap();
        assert_eq!(consumed.is_empty(), true);
    }
}
