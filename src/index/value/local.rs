// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    index::{IndexOps, ValueIndex},
    table::ImmutableTable,
};
use arcon_state::{
    backend::{
        handles::{ActiveHandle, Handle},
        Backend, ValueState,
    },
    data::Value,
    error::*,
};
use std::{borrow::Cow, sync::Arc};

pub struct LocalValue<V, B>
where
    V: Value,
    B: Backend,
{
    /// The data itself
    data: Option<V>,
    /// Modified flag
    modified: bool,
    /// A handle to the ValueState
    handle: ActiveHandle<B, ValueState<V>>,
}

impl<V, B> LocalValue<V, B>
where
    V: Value,
    B: Backend,
{
    /// Creates a LocalValue
    pub fn new(id: impl Into<String>, backend: Arc<B>) -> Self {
        let mut handle = Handle::value(id.into());
        backend.register_value_handle(&mut handle);

        let handle = handle.activate(backend);

        // Attempt to fetch data from backend, otherwise set to default value..
        let data = match handle.get() {
            Ok(Some(v)) => v,
            Ok(None) => V::default(),
            Err(_) => V::default(),
        };

        Self {
            data: Some(data),
            modified: false,
            handle,
        }
    }
}

impl<V, B> ValueIndex<V> for LocalValue<V, B>
where
    V: Value,
    B: Backend,
{
    fn put(&mut self, value: V) -> Result<()> {
        self.data = Some(value);
        self.modified = true;
        Ok(())
    }
    fn get(&self) -> Result<Option<Cow<V>>> {
        Ok(self.data.as_ref().map(|v| Cow::Borrowed(v)))
    }
    fn take(&mut self) -> Result<Option<V>> {
        let data = self.data.take();
        let _ = self.handle.clear();
        Ok(data)
    }
    fn clear(&mut self) -> Result<()> {
        let _ = self.take()?;
        Ok(())
    }
    fn rmw<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(&mut V) + Sized,
    {
        if let Some(ref mut v) = self.data.as_mut() {
            // execute the modification
            f(v);
            // assume the data has actually been modified
            self.modified = true;
        } else {
            self.data = Some(V::default());
        }

        Ok(())
    }
}

impl<V, B> IndexOps for LocalValue<V, B>
where
    V: Value,
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
    fn set_key(&mut self, _: u64) {}
    fn table(&mut self) -> Result<Option<ImmutableTable>> {
        Ok(None)
    }
}
