// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    backend::Backend,
    data::Value,
    error::*,
    index::{HashTable, IndexOps, ValueIndex},
};
use std::{borrow::Cow, sync::Arc};

mod eager;
mod local;

pub use eager::EagerValue;
pub use local::LocalValue;

/// A Lazy ValueIndex
pub struct LazyValue<V, B>
where
    V: Value,
    B: Backend,
{
    current_key: u64,
    hash_table: HashTable<u64, V, B>,
}

impl<V, B> LazyValue<V, B>
where
    V: Value,
    B: Backend,
{
    /// Creates a LazyValue
    pub fn new(id: impl Into<String>, backend: Arc<B>) -> Self {
        let hash_table = HashTable::new(id.into(), backend);

        Self {
            current_key: 0,
            hash_table,
        }
    }
}

impl<V, B> ValueIndex<V> for LazyValue<V, B>
where
    V: Value,
    B: Backend,
{
    #[inline]
    fn put(&mut self, value: V) -> Result<()> {
        self.hash_table.put(self.current_key, value)
    }
    #[inline]
    fn get(&self) -> Result<Option<Cow<V>>> {
        let value = self.hash_table.get(&self.current_key)?;
        Ok(value.map(|v| Cow::Borrowed(v)))
    }
    #[inline]
    fn take(&mut self) -> Result<Option<V>> {
        self.hash_table.remove(&self.current_key)
    }
    #[inline]
    fn clear(&mut self) -> Result<()> {
        let _ = self.take()?;
        Ok(())
    }
    #[inline]
    fn rmw<F>(&mut self, f: F) -> Result<()>
    where
        F: FnMut(&mut V) + Sized,
    {
        self.hash_table.rmw(&self.current_key, V::default, f)
    }
}

impl<V, B> IndexOps for LazyValue<V, B>
where
    V: Value,
    B: Backend,
{
    #[inline]
    fn persist(&mut self) -> Result<()> {
        self.hash_table.persist()
    }
    #[inline]
    fn set_key(&mut self, key: u64) {
        self.current_key = key;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eager::EagerValue;
    use std::sync::Arc;

    fn index_test(mut index: impl ValueIndex<u64>) -> Result<()> {
        index.set_key(0);
        assert_eq!(index.get().unwrap(), None);
        index.put(10u64)?;
        let curr_value = index.get()?;
        assert_eq!(curr_value.unwrap().as_ref(), &10u64);
        index.rmw(|v| {
            *v += 10;
        })?;
        let curr_value = index.get()?;
        assert_eq!(curr_value.unwrap().as_ref(), &20u64);

        index.set_key(1);
        assert_eq!(index.get().unwrap(), None);
        index.put(5u64)?;
        index.clear()?;
        assert_eq!(index.get().unwrap(), None);

        index.set_key(0);
        let removed_value = index.take()?;
        assert_eq!(removed_value, Some(20u64));

        Ok(())
    }

    #[test]
    fn lazy_value_index_test() {
        let backend = Arc::new(crate::backend::temp_backend());
        let index: LazyValue<u64, _> = LazyValue::new("myvalue", backend);
        assert_eq!(index_test(index).is_ok(), true);
    }
    #[test]
    fn eager_value_index_test() {
        let backend = Arc::new(crate::backend::temp_backend());
        let index: EagerValue<u64, _> = EagerValue::new("myvalue", backend);
        assert_eq!(index_test(index).is_ok(), true);
    }
}
