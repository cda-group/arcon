// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{index::IndexOps, table::ImmutableTable};
use arcon_state::{
    backend::{
        handles::{ActiveHandle, BoxedIteratorOfResult, Handle},
        Backend, MapState,
    },
    data::{Key, Value},
    error::*,
};
use std::sync::Arc;

pub struct EagerHashTable<K, V, B>
where
    K: Key,
    V: Value,
    B: Backend,
{
    /// Map Handle
    handle: ActiveHandle<B, MapState<K, V>>,
}

impl<K, V, B> EagerHashTable<K, V, B>
where
    K: Key,
    V: Value,
    B: Backend,
{
    pub fn new(id: impl Into<String>, backend: Arc<B>) -> Self {
        let mut handle = Handle::map(id.into());
        backend.register_map_handle(&mut handle);
        let handle = handle.activate(backend);
        Self { handle }
    }
    /// Insert a key-value record
    #[inline(always)]
    pub fn put(&mut self, key: K, value: V) -> Result<()> {
        self.handle.fast_insert(key, value)
    }

    #[inline(always)]
    pub fn get(&self, k: &K) -> Result<Option<V>> {
        self.handle.get(k)
    }

    #[inline(always)]
    pub fn remove(&self, k: &K) -> Result<Option<V>> {
        self.handle.remove(k)
    }
    #[inline(always)]
    pub fn contains(&self, k: &K) -> Result<bool> {
        self.handle.contains(k)
    }
    #[inline(always)]
    pub fn iter(&self) -> Result<BoxedIteratorOfResult<(K, V)>> {
        self.handle.iter()
    }
}

impl<K, V, B> IndexOps for EagerHashTable<K, V, B>
where
    K: Key,
    V: Value,
    B: Backend,
{
    fn persist(&mut self) -> Result<()> {
        Ok(())
    }
    fn set_key(&mut self, _: u64) {}
    fn table(&mut self) -> Result<Option<ImmutableTable>> {
        Ok(None)
    }
}
