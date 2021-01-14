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

#[derive(Debug)]
pub struct EagerAppender<V, B>
where
    V: Value,
    B: Backend,
{
    /// A handle to the VecState
    handle: ActiveHandle<B, VecState<V>>,
}

impl<V, B> EagerAppender<V, B>
where
    V: Value,
    B: Backend,
{
    /// Creates an EagerAppender
    pub fn new(id: impl Into<String>, backend: Arc<B>) -> Self {
        let mut handle = Handle::vec(id.into());
        backend.register_vec_handle(&mut handle);
        let handle = handle.activate(backend);
        EagerAppender { handle }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.handle.len().unwrap_or(0)
    }

    #[inline(always)]
    pub fn clear(&mut self) -> Result<()> {
        self.handle.clear()
    }

    /// Consume the whole batch of data
    #[inline(always)]
    pub fn consume(&mut self) -> Result<Vec<V>> {
        let stored = self.handle.get()?;
        self.handle.clear()?;
        Ok(stored)
    }

    #[inline(always)]
    pub fn append(&mut self, data: V) -> Result<()> {
        self.handle.append(data)
    }
}

impl<V, B> IndexOps for EagerAppender<V, B>
where
    V: Value,
    B: Backend,
{
    fn persist(&mut self) -> Result<()> {
        Ok(())
    }
}
