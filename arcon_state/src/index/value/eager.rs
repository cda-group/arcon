// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    backend::{
        handles::{ActiveHandle, Handle},
        Backend, ValueState,
    },
    data::Value,
    error::*,
    index::{IndexOps, ValueIndex},
};
use std::{borrow::Cow, sync::Arc};

pub struct EagerValue<V, B>
where
    V: Value,
    B: Backend,
{
    /// A handle to the ValueState
    handle: ActiveHandle<B, ValueState<V>, u64>,
}

impl<V, B> EagerValue<V, B>
where
    V: Value,
    B: Backend,
{
    /// Creates an EagerValue
    pub fn new(id: impl Into<String>, backend: Arc<B>) -> Self {
        let mut handle = Handle::value(id.into()).with_item_key(0);
        backend.register_value_handle(&mut handle);

        let handle: ActiveHandle<B, ValueState<V>, u64> = handle.activate(backend);

        EagerValue { handle }
    }
}

impl<V, B> ValueIndex<V> for EagerValue<V, B>
where
    V: Value,
    B: Backend,
{
    fn put(&mut self, value: V) -> Result<()> {
        self.handle.fast_set(value)
    }
    fn get(&self) -> Result<Option<Cow<V>>> {
        let value = self.handle.get()?;
        Ok(value.map(|v| Cow::Owned(v)))
    }
    fn take(&mut self) -> Result<Option<V>> {
        let value = self.handle.get()?;
        self.clear()?;
        Ok(value)
    }
    fn clear(&mut self) -> Result<()> {
        self.handle.clear()
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
    V: Value,
    B: Backend,
{
    fn persist(&mut self) -> Result<()> {
        Ok(())
    }
    fn set_key(&mut self, key: u64) {
        self.handle.set_item_key(key);
    }
}
