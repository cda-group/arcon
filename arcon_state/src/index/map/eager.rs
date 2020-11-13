use crate::{
    backend::{
        handles::{ActiveHandle, BoxedIteratorOfResult},
        Backend, MapState,
    },
    data::{Key, Value},
    error::*,
    index::IndexOps,
};

pub struct EagerMap<K, V, B>
where
    K: Key,
    V: Value,
    B: Backend,
{
    /// Map Handle
    handle: ActiveHandle<B, MapState<K, V>>,
}

impl<K, V, B> EagerMap<K, V, B>
where
    K: Key,
    V: Value,
    B: Backend,
{
    pub fn new(handle: ActiveHandle<B, MapState<K, V>>) -> Self {
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
    pub fn remove(&self, k: &K) -> Result<()> {
        self.handle.fast_remove(k)
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

impl<K, V, B> IndexOps for EagerMap<K, V, B>
where
    K: Key,
    V: Value,
    B: Backend,
{
    fn persist(&mut self) -> Result<()> {
        Ok(())
    }
}
