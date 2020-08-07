// Copyright (c) 2016 Amanieu d'Antras
// SPDX-License-Identifier: MIT

// Modifications Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    borrow::Borrow,
    hash::{BuildHasher, Hash, Hasher},
};

use crate::{error::*, handles::Handle, hint::unlikely, index::IndexOps, Key, MapState, Value};

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
use crate::{Backend, BackendContainer};
use std::{cell::UnsafeCell, rc::Rc};

// Set FxHash to default as most keys tend to be small
pub type DefaultHashBuilder = fxhash::FxBuildHasher;

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
    ///
    /// The function will evict a bucket if the table is above the given
    /// modification threshold.
    #[inline]
    fn insert(&self, k: K, v: V) -> Result<Option<V>> {
        let hash = make_hash(&self.hash_builder, &k);
        let table = self.raw_table_mut();
        unsafe {
            // If the entry is already in the RawTable then
            // replace it with new one. Otherwise, insert the
            // new entry.
            if let Some(item) = table.find_mut(hash, |x| k.eq(&x.0)) {
                Ok(Some(std::mem::replace(&mut item.as_mut().1, v)))
            } else {
                // If we are above the modification threshold, then
                // move a modified entry to the backend.
                if table.above_mod_threshold() {
                    let bucket = table.evict_mod_bucket(hash);
                    let &(ref key, ref value) = bucket.as_ref();
                    self.backend_put(key.clone(), value.clone())?;
                }
                // continue with insert
                table.insert(hash, (k, v));
                Ok(None)
            }
        }
    }

    /// Internal helper to get a value from the Backend
    #[inline]
    fn backend_get(&self, k: &K) -> Result<Option<V>> {
        let mut sb_session = self.backend.session();
        let state = self.handle.activate(&mut sb_session);
        state.get(k)
    }

    /// Internal helper to put a key-value record into the Backend
    #[inline]
    fn backend_put(&self, k: K, v: V) -> Result<()> {
        let mut sb_session = self.backend.session();
        let mut state = self.handle.activate(&mut sb_session);
        state.fast_insert(k, v)
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

    #[inline(always)]
    pub fn get(&self, key: &K) -> Result<Option<&V>> {
        let entry = self.table_get(key);

        // Return early if we have a match on our RawTable
        if entry.is_some() {
            return Ok(entry);
        }

        // Attempt to find the value in the Backend
        let entry_opt = self.backend_get(key)?;
        if let Some(v) = entry_opt {
            // Insert the value back into the index
            self.insert(key.clone(), v)?;
            // Kinda silly but run table_get again to get the referenced value.
            // Cannot return a referenced value created in the function itself...
            Ok(self.table_get(key))
        } else {
            // The key does not exist
            Ok(None)
        }
    }
    #[inline(always)]
    pub fn put(&mut self, key: K, value: V) -> Result<()> {
        self.insert(key, value)?;
        Ok(())
    }

    #[inline(always)]
    pub fn rmw<F: Sized>(&mut self, key: &K, mut f: F) -> Result<bool>
    where
        F: FnMut(&mut V),
    {
        if let Some(mut entry) = self.table_get_mut(key) {
            // run the udf on the data
            f(&mut entry);

            // as we have touched `key` through table_get_mut,
            // check whether we are above the modifcation limit,
            // and proceed to evict bucket if that is the case.
            let table = self.raw_table_mut();
            if unlikely(table.above_mod_threshold()) {
                unsafe {
                    let hash = make_hash(&self.hash_builder, &key);
                    let bucket = table.evict_mod_bucket(hash);
                    let &(ref key, ref value) = bucket.as_ref();
                    self.backend_put(key.clone(), value.clone())?;
                };
            }

            // indicate that the operation was successful
            return Ok(true);
        }

        // Attempt to find the value in the Backend
        let entry_opt = self.backend_get(key)?;
        if let Some(mut value) = entry_opt {
            // run the rmw op on the value
            f(&mut value);
            // insert the value into the RawTable
            self.insert(key.clone(), value)?;
            // indicate that the operation was successful
            return Ok(true);
        } else {
            // return false as the rmw operation did not modify the given key
            return Ok(false);
        }
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
            for bucket in table.iter_modified() {
                let &(ref key, ref value) = bucket.as_ref();
                map_state.fast_insert(key.clone(), value.clone())?;
            }
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
            hash_index.put(i as u64, i as u64);
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
}
