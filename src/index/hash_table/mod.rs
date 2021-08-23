// Copyright (c) 2016 Amanieu d'Antras
// SPDX-License-Identifier: MIT

// Modifications Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    borrow::Borrow,
    hash::{BuildHasher, Hash, Hasher},
};

use crate::{error::ArconResult, index::IndexOps, table::ImmutableTable};
use arcon_state::{
    backend::{
        handles::{ActiveHandle, Handle},
        MapState,
    },
    data::{Key, Value},
    error::*,
};
use std::sync::Arc;

cfg_if::cfg_if! {
    // Use the SSE2 implementation if possible: it allows us to scan 16 buckets
    // at once instead of 8. We don't bother with AVX since it would require
    // runtime dispatch and wouldn't gain us much anyways: the probability of
    // finding a match drops off drastically after the first few buckets.
    //
    // I attempted an implementation on ARM using NEON instructions, but it
    // turns out that most NEON instructions have multi-cycle latency, which in
    // the end outweighs any gains over the generic implementation.
    if #[cfg(all(
        target_feature = "sse2",
        any(target_arch = "x86", target_arch = "x86_64"),
        not(miri)
    ))] {
        mod sse2; use sse2 as imp;
    } else {
        panic!("sse2 needed for now");
        #[path = "generic.rs"]
        mod generic;
        use generic as imp;
    }
}

mod bitmask;
pub mod eager;
mod table;

#[cfg(test)]
use self::table::TableModIterator;
use self::table::{ProbeModIterator, RawTable};
use crate::backend::Backend;
use std::cell::UnsafeCell;

const DEFAULT_READ_LANE_SIZE: usize = 8192;
const DEFAULT_MOD_LANE_SIZE: usize = 1024;

// Set FxHash to default as most keys tend to be small
pub type DefaultHashBuilder = fxhash::FxBuildHasher;

/// A HashTable suitable for point lookups and in-place
/// updates of hot values. It holds a handle to a MapState
/// type where it may persist or fetch data from.
pub struct HashTable<K, V, B>
where
    K: Key + Hash,
    V: Value,
    B: Backend,
{
    /// Hasher for the keys
    hash_builder: fxhash::FxBuildHasher,
    /// In-memory RawTable
    raw_table: UnsafeCell<RawTable<K, V>>,
    /// HashTable Handle
    handle: ActiveHandle<B, MapState<K, V>>,
}

#[inline]
pub(crate) fn make_hash<K: Hash + ?Sized>(hash_builder: &impl BuildHasher, val: &K) -> u64 {
    let mut state = hash_builder.build_hasher();
    val.hash(&mut state);
    state.finish()
}

impl<K, V, B> HashTable<K, V, B>
where
    K: Key + Eq + Hash,
    V: Value,
    B: Backend,
{
    /// Creates a HashTable with default settings
    pub fn new(id: impl Into<String>, backend: Arc<B>) -> Self {
        let mut handle = Handle::map(id.into());
        backend.register_map_handle(&mut handle);
        let handle = handle.activate(backend);

        HashTable {
            hash_builder: DefaultHashBuilder::default(),
            raw_table: UnsafeCell::new(RawTable::with_capacity(
                DEFAULT_MOD_LANE_SIZE,
                DEFAULT_READ_LANE_SIZE,
            )),
            handle,
        }
    }

    /// Creates a HashTable with specified capacities
    pub fn with_capacity(
        id: impl Into<String>,
        backend: Arc<B>,
        mod_capacity: usize,
        read_capacity: usize,
    ) -> Self {
        assert!(mod_capacity.is_power_of_two());
        assert!(read_capacity.is_power_of_two());

        let mut handle = Handle::map(id.into());
        backend.register_map_handle(&mut handle);
        let handle = handle.activate(backend);

        HashTable {
            hash_builder: DefaultHashBuilder::default(),
            raw_table: UnsafeCell::new(RawTable::with_capacity(mod_capacity, read_capacity)),
            handle,
        }
    }

    /// Internal helper function to access a RawTable
    #[inline(always)]
    fn raw_table(&self) -> &RawTable<K, V> {
        unsafe { &*self.raw_table.get() }
    }

    /// Internal helper function to access a mutable RawTable
    #[inline(always)]
    #[allow(clippy::mut_from_ref)]
    fn raw_table_mut(&self) -> &mut RawTable<K, V> {
        unsafe { &mut *self.raw_table.get() }
    }

    /// Insert a Key-Value record into the RawTable
    #[inline(always)]
    fn insert(&self, k: K, v: V, hash: u64) -> Result<()> {
        let table = self.raw_table_mut();

        // If the key exists in the mod lane already, we simply update the value..
        if let Some(item) = table.find_mod_lane_mut(hash, |x| k.eq(x.0)) {
            *item = v;
        } else if let Some((mod_iter, (k, v))) = table.insert_mod_lane(hash, (k, v)) {
            self.drain_modified(mod_iter)?;
            // This shall not fail now
            let _ = table.insert_mod_lane(hash, (k, v));
        }
        Ok(())
    }

    /// Insert a Key-Value record into the RawTable
    #[inline(always)]
    fn insert_read_lane(&self, k: K, v: V, hash: u64) {
        let table = self.raw_table_mut();
        table.insert_read_lane(hash, (k, v));
    }

    /// Internal helper to get a value from the Backend
    #[inline]
    fn backend_get(&self, k: &K) -> Result<Option<V>> {
        self.handle.get(k)
    }

    /// Internal helper to delete a key-value record from the Backend
    ///
    /// This version returns the deleted value if it existed before
    #[inline]
    fn backend_remove(&self, k: &K) -> Result<Option<V>> {
        self.handle.remove(k)
    }

    /// Internal helper to delete a key-value record from the Backend
    ///
    /// This version does not return a possible old value
    #[inline]
    fn backend_remove_fast(&self, k: &K) -> Result<()> {
        self.handle.fast_remove(k)
    }

    #[inline]
    fn table_get<Q: ?Sized>(&self, k: &Q, hash: u64) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let table = self.raw_table();
        table.find(hash, |x| k.eq(x.0.borrow())).map(|(_, v)| v)
    }

    #[inline(always)]
    fn table_find_mod_lane<Q: ?Sized>(&mut self, k: &Q, hash: u64) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let table = self.raw_table_mut();
        table.find_mod_lane_mut(hash, |x| k.eq(x.0.borrow()))
    }

    #[inline(always)]
    fn table_take_read_lane<Q: ?Sized>(&mut self, k: &Q, hash: u64) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let table = self.raw_table_mut();
        table.take_read_lane(hash, |x| k.eq(x.0.borrow()))
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.raw_table().len()
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.raw_table().len() == 0
    }

    /// Remove a value by Key and return existing item if found
    #[inline(always)]
    pub fn remove(&mut self, k: &K) -> Result<Option<V>> {
        // (1). Probe RawTable and Remove if it exists
        // (2). Delete from Backend

        let table = self.raw_table_mut();
        let hash = make_hash(&self.hash_builder, &k);

        match table.remove(hash, |x| k.eq(x.0.borrow())) {
            Some(item) => {
                self.backend_remove_fast(k)?;
                Ok(Some(item.1))
            }
            None => {
                // Key was not found in RawTable, attempt to remove from the backend
                self.backend_remove(k)
            }
        }
    }

    /// Fetch a value by Key
    #[inline(always)]
    pub fn get(&self, key: &K) -> Result<Option<&V>> {
        let hash = make_hash(&self.hash_builder, key);

        // Attempt to find the value by probing the RawTable
        let entry = self.table_get(key, hash);

        // Return early if we have a match
        if entry.is_some() {
            return Ok(entry);
        }

        // Attempt to find the value in the Backend
        match self.backend_get(key)? {
            Some(v) => {
                // Insert the value back into the index
                self.insert_read_lane(key.clone(), v, hash);
                // Kinda silly but run table_get again to get the referenced value.
                // Cannot return a referenced value created in the function itself...
                Ok(self.table_get(key, hash))
            }
            None => {
                // Key does not exist
                Ok(None)
            }
        }
    }

    /// Insert a key-value record into the RawTable
    #[inline(always)]
    pub fn put(&mut self, key: K, value: V) -> Result<()> {
        let hash = make_hash(&self.hash_builder, &key);
        self.insert(key, value, hash)
    }

    /// Read-Modify-Write Operation
    ///
    /// The `F` function will execute in-place if the value is found in the RawTable.
    /// Otherwise, there will be an attempt to find the value in the Backend.
    ///
    /// The `P` function defines how a default value is created if there is
    /// no entry in the HashTable.
    #[inline(always)]
    pub fn rmw<F: Sized, P>(&mut self, key: &K, p: P, f: F) -> Result<()>
    where
        F: FnOnce(&mut V),
        P: FnOnce() -> V,
    {
        let hash = make_hash(&self.hash_builder, key);
        // In best case, we find the record in the RawTable's MOD lane
        // and modify the record in place.
        if let Some(mut entry) = self.table_find_mod_lane(key, hash) {
            // run the udf on the data
            f(&mut entry);
            return Ok(());
        }

        // Attempt to find the value in the READ lane. If found,
        // we modify the value before inserting it back into the MOD lane..
        if let Some((key, mut value)) = self.table_take_read_lane(key, hash) {
            f(&mut value);
            self.insert(key, value, hash)?;
            return Ok(());
        }

        // Otherwise, check the backing state backend if it has the key.
        match self.backend_get(key)? {
            Some(mut value) => {
                // run the rmw op on the value
                f(&mut value);
                // insert the value into the RawTable
                self.insert(key.clone(), value, hash)?;
            }
            None => {
                let mut value = p();
                f(&mut value);
                self.insert(key.clone(), value, hash)?;
            }
        }

        Ok(())
    }

    /// Inserts Modified elements in a MOD lane probe sequence into the
    /// backing MapState.
    #[inline(always)]
    pub fn drain_modified(&self, iter: ProbeModIterator<K, V>) -> Result<()> {
        self.handle.insert_all_by_ref(iter)
    }

    #[allow(clippy::type_complexity)]
    pub fn full_iter(&mut self) -> ArconResult<(usize, Box<dyn Iterator<Item = Result<V>> + '_>)> {
        // call our persist method to force possible modified values to the backend
        self.persist()?;
        let len = self.handle.len()?;
        let values = self.handle.values()?;
        Ok((len, values))
    }

    /// Method only used for testing the TableModIterator of RawTable.
    #[cfg(test)]
    pub(crate) fn modified_iterator(&mut self) -> TableModIterator<K, V> {
        let table = self.raw_table_mut();
        unsafe { table.iter_modified() }
    }
}

impl<K, V, B> IndexOps for HashTable<K, V, B>
where
    K: Key + Eq + Hash,
    V: Value,
    B: Backend,
{
    fn persist(&mut self) -> ArconResult<()> {
        let table = self.raw_table_mut();
        unsafe {
            self.handle.insert_all_by_ref(table.iter_modified())?;
        };
        Ok(())
    }
    fn set_key(&mut self, _: u64) {}
    fn table(&mut self) -> ArconResult<Option<ImmutableTable>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::temp_backend;
    use arcon_state::backend::sled::Sled;
    use std::sync::Arc;

    #[test]
    fn basic_test() {
        let backend = Arc::new(temp_backend());

        let mod_capacity = 1024;
        let read_capacity = 1024;
        let mut hash_index: HashTable<u64, u64, Sled> =
            HashTable::with_capacity("table", backend, mod_capacity, read_capacity);

        for i in 0..1024 {
            let key: u64 = i as u64;
            hash_index.rmw(&key, || key, |v| *v += 1).expect("failure");
        }
        for i in 0..1024 {
            let key: u64 = i as u64;
            assert_eq!(hash_index.get(&key).unwrap(), Some(&(key + 1)));
        }
        assert!(hash_index.persist().is_ok());
    }

    #[test]
    fn modified_test() {
        let backend = Arc::new(temp_backend());
        let capacity = 64;

        let mut hash_index: HashTable<u64, u64, Sled> =
            HashTable::with_capacity("table", backend, capacity, capacity);
        for i in 0..10 {
            hash_index.put(i as u64, i as u64).unwrap();
        }

        assert_eq!(hash_index.modified_iterator().count(), 10);

        // The meta data is reset, so the counter should now be zero
        assert_eq!(hash_index.modified_iterator().count(), 0);

        // Run rmw operation on the following keys and check that they are indeed
        // returned from our modified_iterator.
        let rmw_keys = vec![0, 1, 2];
        for key in &rmw_keys {
            assert!(hash_index.rmw(key, || 0, |v| *v += 1).is_ok());
        }

        for (key, value) in hash_index.modified_iterator() {
            assert!(rmw_keys.contains(key));
            assert_eq!(value, &(key + 1));
        }
    }
}
