// Copyright (c) 2016 Amanieu d'Antras
// SPDX-License-Identifier: MIT

// Modifications Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    borrow::Borrow,
    hash::{BuildHasher, Hash, Hasher},
};

use crate::{error::*, handles::Handle, index::IndexOps, Key, MapState, Value};

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
mod table;

use self::table::RawTable;
#[cfg(test)]
use crate::index::hash::table::ModifiedIterator;
use crate::{index::hash::table::ModIterator, Backend, BackendContainer};
use std::{cell::UnsafeCell, rc::Rc};

// Set FxHash to default as most keys tend to be small
pub type DefaultHashBuilder = fxhash::FxBuildHasher;

/// A HashIndex suitable for point lookups and in-place
/// updates of hot values. It holds a handle to a MapState
/// type where it may persist or fetch data from.
pub struct HashIndex<K, V, B>
where
    K: Key,
    V: Value,
    B: Backend,
{
    /// Hasher for the keys
    hash_builder: fxhash::FxBuildHasher,
    /// In-memory RawTable
    raw_table: UnsafeCell<RawTable<(K, V)>>,
    /// Map Handle
    handle: Handle<MapState<K, V>>,
    /// The underlying backend
    backend: Rc<BackendContainer<B>>,
}

#[inline]
pub(crate) fn make_hash<K: Hash + ?Sized>(hash_builder: &impl BuildHasher, val: &K) -> u64 {
    let mut state = hash_builder.build_hasher();
    val.hash(&mut state);
    state.finish()
}

impl<K, V, B> HashIndex<K, V, B>
where
    K: Key + Eq + Hash,
    V: Value,
    B: Backend,
{
    /// Creates a HashIndex
    pub fn new(
        key: &'static str,
        capacity: usize,
        mod_factor: f32,
        backend: Rc<BackendContainer<B>>,
    ) -> HashIndex<K, V, B> {
        // register handle
        let mut handle = Handle::map(key);
        handle.register(&mut unsafe {
            crate::RegistrationToken::new(&mut backend.clone().session())
        });

        HashIndex {
            hash_builder: DefaultHashBuilder::default(),
            raw_table: UnsafeCell::new(RawTable::with_capacity(capacity, mod_factor)),
            handle,
            backend,
        }
    }

    /// Internal helper function to access a RawTable
    #[inline(always)]
    fn raw_table(&self) -> &RawTable<(K, V)> {
        unsafe { &*self.raw_table.get() }
    }

    /// Internal helper function to access a mutable RawTable
    #[inline(always)]
    fn raw_table_mut(&self) -> &mut RawTable<(K, V)> {
        unsafe { &mut *self.raw_table.get() }
    }

    /// Insert a Key-Value record into the RawTable
    #[inline(always)]
    fn insert(&self, k: K, v: V) -> Result<()> {
        let hash = make_hash(&self.hash_builder, &k);
        let table = self.raw_table_mut();
        unsafe {
            if let Some(item) = table.find_mut(hash, |x| k.eq(&x.0)) {
                item.as_mut().1 = v;
            } else {
                if let Some((mod_iter, (k, v))) = table.insert(hash, (k, v)) {
                    self.drain_modified(mod_iter)?;
                    // This shall not fail now
                    let _ = table.insert(hash, (k, v));
                }
            }
            Ok(())
        }
    }

    /// Internal helper to get a value from the Backend
    #[inline]
    fn backend_get(&self, k: &K) -> Result<Option<V>> {
        let mut sb_session = self.backend.session();
        let state = self.handle.activate(&mut sb_session);
        state.get(k)
    }

    /// Internal helper to delete a key-value record from the Backend
    ///
    /// This version returns the deleted value if it existed before
    #[inline]
    fn backend_remove(&self, k: &K) -> Result<Option<V>> {
        let mut sb_session = self.backend.session();
        let mut state = self.handle.activate(&mut sb_session);
        state.remove(k)
    }

    /// Internal helper to delete a key-value record from the Backend
    ///
    /// This version does not return a possible old value
    #[inline]
    fn backend_remove_fast(&self, k: &K) -> Result<()> {
        let mut sb_session = self.backend.session();
        let mut state = self.handle.activate(&mut sb_session);
        state.fast_remove(k)
    }

    #[inline]
    fn table_get<Q: ?Sized>(&self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.get_key_value(k).map(|(_, v)| v)
    }

    #[inline]
    fn table_get_mut<Q: ?Sized>(&mut self, k: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let hash = make_hash(&self.hash_builder, k);
        let table = self.raw_table_mut();
        table
            .find_mut(hash, |x| k.eq(x.0.borrow()))
            .map(|item| unsafe { &mut item.as_mut().1 })
    }

    #[inline]
    fn get_key_value<Q: ?Sized>(&self, k: &Q) -> Option<(&K, &V)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        let hash = make_hash(&self.hash_builder, k);
        let table = self.raw_table();
        table.find(hash, |x| k.eq(x.0.borrow())).map(|item| unsafe {
            let &(ref key, ref value) = item.as_ref();
            (key, value)
        })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.raw_table().len()
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.raw_table().len() == 0
    }
    #[inline]
    pub fn mod_limit(&self) -> usize {
        self.raw_table().mod_limit()
    }
    #[inline]
    pub fn capacity(&self) -> usize {
        self.raw_table().capacity()
    }
    /// Remove a value by Key
    #[inline(always)]
    pub fn erase(&mut self, k: &K) -> Result<()> {
        // (1). Probe RawTable and erase if it exists
        // (2). Delete from Backend

        let table = self.raw_table_mut();
        let hash = make_hash(&self.hash_builder, &k);
        unsafe {
            if let Some(item) = table.find(hash, |x| k.eq(x.0.borrow())) {
                table.erase(item)
            }
        };
        self.backend_remove_fast(k)
    }

    /// Remove a value by Key and return existing item if found
    #[inline(always)]
    pub fn remove(&mut self, k: &K) -> Result<Option<V>> {
        // (1). Probe RawTable and Remove if it exists
        // (2). Delete from Backend

        let table = self.raw_table_mut();
        let hash = make_hash(&self.hash_builder, &k);

        unsafe {
            match table.find(hash, |x| k.eq(x.0.borrow())) {
                Some(item) => {
                    let value = table.remove(item).1;
                    self.backend_remove_fast(k)?;
                    return Ok(Some(value));
                }
                None => {
                    // Key was not found in RawTable, attempt to remove from the backend
                    return self.backend_remove(k);
                }
            }
        };
    }

    /// Fetch a value by Key
    #[inline(always)]
    pub fn get(&self, key: &K) -> Result<Option<&V>> {
        // Attempt to find the value by probing the RawTable
        let entry = self.table_get(key);

        // Return early if we have a match
        if entry.is_some() {
            return Ok(entry);
        }

        // Attempt to find the value in the Backend
        match self.backend_get(key)? {
            Some(v) => {
                // Insert the value back into the index
                self.insert(key.clone(), v)?;
                // Kinda silly but run table_get again to get the referenced value.
                // Cannot return a referenced value created in the function itself...
                Ok(self.table_get(key))
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
        self.insert(key, value)
    }

    /// Read-Modify-Write Operation
    ///
    /// The `F` function will execute in-place if the value is found in the RawTable.
    /// Otherwise, there will be an attempt to find the value in the Backend.
    #[inline(always)]
    pub fn rmw<F: Sized>(&mut self, key: &K, mut f: F) -> Result<bool>
    where
        F: FnMut(&mut V),
    {
        // Probe the RawTable and modify the record in-place if possible..
        if let Some(mut entry) = self.table_get_mut(key) {
            // run the udf on the data
            f(&mut entry);
            // indicate that the operation was successful
            return Ok(true);
        }

        // Check whether the value is in the backend
        match self.backend_get(key)? {
            Some(mut value) => {
                // run the rmw op on the value
                f(&mut value);
                // insert the value into the RawTable
                self.put(key.clone(), value)?;
                // indicate that the operation was successful
                Ok(true)
            }
            None => {
                // return false as the rmw operation did not modify the given key
                Ok(false)
            }
        }
    }
    #[inline(always)]
    pub fn drain_modified(&self, iter: ModIterator<(K, V)>) -> Result<()> {
        let mut sb_session = self.backend.session();
        let mut map_state = self.handle.activate(&mut sb_session);
        map_state.insert_all_by_ref(iter)
    }

    /// Method only used for testing the ModifiedIterator of RawTable.
    #[cfg(test)]
    pub(crate) fn modified_iterator(&mut self) -> ModifiedIterator<(K, V)> {
        let table = self.raw_table_mut();
        unsafe { table.iter_modified() }
    }
}

impl<K, V, B> IndexOps for HashIndex<K, V, B>
where
    K: Key + Eq + Hash,
    V: Value,
    B: Backend,
{
    fn persist(&mut self) -> Result<()> {
        let table = self.raw_table_mut();
        unsafe {
            let mut sb_session = self.backend.session();
            let mut map_state = self.handle.activate(&mut sb_session);
            map_state.insert_all_by_ref(table.iter_modified())?;
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::in_memory::InMemory;
    use std::rc::Rc;

    #[test]
    fn basic_test() {
        let backend = crate::InMemory::create(&std::path::Path::new("/tmp/")).unwrap();
        let mod_factor: f32 = 0.4;
        let capacity = 4;
        let mut hash_index: HashIndex<u64, u64, InMemory> =
            HashIndex::new("_hashindex", capacity, mod_factor, Rc::new(backend));
        for i in 0..1024 {
            hash_index.put(i as u64, i as u64).unwrap();
            let key: u64 = i as u64;
            assert_eq!(hash_index.get(&key).unwrap(), Some(&key));
        }
        for i in 0..1024 {
            let key: u64 = i as u64;
            assert_eq!(
                hash_index
                    .rmw(&key, |v| {
                        *v += 1;
                    })
                    .unwrap(),
                true
            );
        }
        for i in 0..1024 {
            let key: u64 = i as u64;
            assert_eq!(hash_index.get(&key).unwrap(), Some(&(key + 1)));
        }
        assert_eq!(hash_index.persist().is_ok(), true);
    }

    #[test]
    fn modified_test() {
        let backend = crate::InMemory::create(&std::path::Path::new("/tmp/")).unwrap();
        let mod_factor: f32 = 0.4;
        let capacity = 10;

        let mut hash_index: HashIndex<u64, u64, InMemory> =
            HashIndex::new("_hashindex", capacity, mod_factor, Rc::new(backend));
        for i in 0..10 {
            hash_index.put(i as u64, i as u64).unwrap();
        }

        // modified limit is 0.4, while our capacity is 10, the
        // RawTable will calculate a suitable power of two capacity.
        // In this case, the underlying capacity is set to 14.
        // modified limit is thus set to 5 (14*0.4).

        assert_eq!(hash_index.capacity(), 14);
        assert_eq!(hash_index.modified_iterator().count(), 5);

        // The meta data is reset, so the counter should now be zero
        assert_eq!(hash_index.modified_iterator().count(), 0);

        // Run rmw operation on the following keys and check that they are indeed
        // returned from our modified_iterator.
        let rmw_keys = vec![0, 1, 2];
        for key in &rmw_keys {
            assert_eq!(
                hash_index
                    .rmw(&key, |v| {
                        *v += 1;
                    })
                    .unwrap(),
                true
            );
        }

        for (key, value) in hash_index.modified_iterator() {
            assert_eq!(rmw_keys.contains(&key), true);
            assert_eq!(value, &(key + 1));
        }
    }
}
