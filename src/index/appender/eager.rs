// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

#[cfg(feature = "arcon_arrow")]
use crate::data::arrow::ArrowTable;
use crate::index::{AppenderIndex, IndexOps};
use arcon_state::{
    backend::{
        handles::{ActiveHandle, Handle},
        Backend, VecState,
    },
    data::Value,
    error::*,
};
use std::sync::Arc;

#[derive(Debug)]
pub struct EagerAppender<V, B>
where
    V: Value,
    B: Backend,
{
    /// A handle to the VecState
    handle: ActiveHandle<B, VecState<V>, u64>,
}

impl<V, B> EagerAppender<V, B>
where
    V: Value,
    B: Backend,
{
    /// Creates an EagerAppender
    pub fn new(id: impl Into<String>, backend: Arc<B>) -> Self {
        let mut handle = Handle::vec(id.into()).with_item_key(0);
        backend.register_vec_handle(&mut handle);
        let handle: ActiveHandle<B, VecState<V>, u64> = handle.activate(backend);
        EagerAppender { handle }
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
    fn set_key(&mut self, key: u64) {
        self.handle.set_item_key(key);
    }
    #[cfg(feature = "arcon_arrow")]
    fn arrow_table(&mut self) -> Result<Option<ArrowTable>> {
        Ok(None)
    }
}

impl<V, B> AppenderIndex<V> for EagerAppender<V, B>
where
    V: Value,
    B: Backend,
{
    #[inline]
    fn append(&mut self, data: V) -> Result<()> {
        self.handle.append(data)
    }
    #[inline]
    fn consume(&mut self) -> Result<Vec<V>> {
        let stored = self.handle.get()?;
        self.handle.clear()?;
        Ok(stored)
    }
    #[inline]
    fn len(&self) -> usize {
        self.handle.len().unwrap_or(0)
    }
    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
