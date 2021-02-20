// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

#[cfg(feature = "arcon_arrow")]
use crate::data::arrow::ArrowTable;
use crate::index::{IndexOps, IndexValue, ValueIndex};
use arcon_state::{
    backend::{
        handles::{ActiveHandle, Handle},
        Backend, MapState,
    },
    error::*,
};
use std::{borrow::Cow, sync::Arc};

pub struct EagerValue<V, B>
where
    V: IndexValue,
    B: Backend,
{
    /// A handle to the ValueState
    handle: ActiveHandle<B, MapState<u64, V>>,
    current_key: u64,
}

impl<V, B> EagerValue<V, B>
where
    V: IndexValue,
    B: Backend,
{
    /// Creates an EagerValue
    pub fn new(id: impl Into<String>, backend: Arc<B>) -> Self {
        let mut handle = Handle::map(id.into());
        backend.register_map_handle(&mut handle);

        let handle: ActiveHandle<B, MapState<u64, V>> = handle.activate(backend);

        EagerValue {
            handle,
            current_key: 0,
        }
    }
}

impl<V, B> ValueIndex<V> for EagerValue<V, B>
where
    V: IndexValue,
    B: Backend,
{
    fn put(&mut self, value: V) -> Result<()> {
        self.handle.fast_insert(self.current_key, value)
    }
    fn get(&self) -> Result<Option<Cow<V>>> {
        let value = self.handle.get(&self.current_key)?;
        Ok(value.map(|v| Cow::Owned(v)))
    }
    fn take(&mut self) -> Result<Option<V>> {
        self.handle.remove(&self.current_key)
    }
    fn clear(&mut self) -> Result<()> {
        let _ = self.take()?;
        Ok(())
    }
    fn rmw<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(&mut V) + Sized,
    {
        let value = self.get()?;
        if let Some(v) = value {
            let mut owned = v.into_owned();
            f(&mut owned);
            self.put(owned)
        } else {
            self.put(V::default())
        }
    }
}

impl<V, B> IndexOps for EagerValue<V, B>
where
    V: IndexValue,
    B: Backend,
{
    fn persist(&mut self) -> Result<()> {
        Ok(())
    }
    fn set_key(&mut self, key: u64) {
        self.current_key = key;
    }
    #[cfg(feature = "arcon_arrow")]
    fn arrow_table(&mut self) -> Result<Option<ArrowTable>> {
        let len = self.handle.len()?;
        let mut table = V::arrow_table(len);
        let values = self.handle.values()?;
        table
            .load(values.filter_map(|v| v.ok()))
            .map_err(|e| ArconStateError::Unknown { msg: e.to_string() })?;
        Ok(Some(table))
    }
}
