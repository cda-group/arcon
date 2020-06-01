// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
use crate::{
    error::*,
    handles::BoxedIteratorOfResult,
    rocks::default_write_opts,
    serialization::{fixed_bytes, protobuf},
    Handle, Key, MapOps, MapState, Metakey, Rocks, Value,
};
use rocksdb::WriteBatch;

impl MapOps for Rocks {
    fn map_clear<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<()> {
        let prefix = handle.serialize_metakeys()?;
        self.remove_prefix(handle.id, prefix)
    }

    fn map_get<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<Option<V>> {
        let key = handle.serialize_metakeys_and_key(key)?;
        if let Some(serialized) = self.get(handle.id, &key)? {
            let value = protobuf::deserialize(&serialized)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn map_fast_insert<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: K,
        value: V,
    ) -> Result<()> {
        let key = handle.serialize_metakeys_and_key(&key)?;
        let serialized = protobuf::serialize(&value)?;
        self.put(handle.id, key, serialized)?;

        Ok(())
    }

    fn map_insert<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: K,
        value: V,
    ) -> Result<Option<V>> {
        let key = handle.serialize_metakeys_and_key(&key)?;

        // couldn't find a `put` that would return the previous value from rocks
        let old = if let Some(slice) = self.get(handle.id, &key)? {
            Some(protobuf::deserialize(&slice[..])?)
        } else {
            None
        };

        let serialized = protobuf::serialize(&value)?;
        self.put(handle.id, key, serialized)?;

        Ok(old)
    }

    fn map_insert_all<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key_value_pairs: impl IntoIterator<Item = (K, V)>,
    ) -> Result<()> {
        let backend = self.initialized_mut()?;

        let mut wb = WriteBatch::default();
        let cf = backend.get_cf_handle(handle.id)?;

        for (user_key, value) in key_value_pairs {
            let key = handle.serialize_metakeys_and_key(&user_key)?;
            let serialized = protobuf::serialize(&value)?;
            wb.put_cf(cf, key, serialized)?;
        }

        Ok(backend.db.write_opt(wb, &default_write_opts())?)
    }

    fn map_remove<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<Option<V>> {
        let key = handle.serialize_metakeys_and_key(key)?;

        let old = if let Some(slice) = self.get(handle.id, &key)? {
            Some(protobuf::deserialize(&slice[..])?)
        } else {
            None
        };

        self.remove(handle.id, &key)?;

        Ok(old)
    }

    fn map_fast_remove<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<()> {
        let key = handle.serialize_metakeys_and_key(key)?;
        self.remove(handle.id, &key)?;

        Ok(())
    }

    fn map_contains<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<bool> {
        let key = handle.serialize_metakeys_and_key(key)?;
        self.contains(handle.id, &key)
    }

    fn map_iter<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, (K, V)>> {
        let backend = self.initialized()?;

        let prefix = handle.serialize_metakeys()?;
        let cf = backend.get_cf_handle(handle.id)?;
        // NOTE: prefix_iterator only works as expected when the cf has proper prefix_extractor
        //   option set. We do that in Rocks::register_*_state
        let iter =
            backend
                .db
                .prefix_iterator_cf(cf, prefix)?
                .map(move |(db_key, serialized_value)| {
                    let mut key_cursor = &db_key[..];
                    let _item_key: IK = fixed_bytes::deserialize_from(&mut key_cursor)?;
                    let _namespace: N = fixed_bytes::deserialize_from(&mut key_cursor)?;
                    let key: K = protobuf::deserialize_from(&mut key_cursor)?;
                    let value: V = protobuf::deserialize(&serialized_value)?;

                    Ok((key, value))
                });

        Ok(Box::new(iter))
    }

    fn map_keys<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, K>> {
        let backend = self.initialized()?;

        let prefix = handle.serialize_metakeys()?;
        let cf = backend.get_cf_handle(handle.id)?;

        let iter = backend
            .db
            .prefix_iterator_cf(cf, prefix)?
            .map(move |(db_key, _)| {
                let mut key_cursor = &db_key[..];
                let _item_key: IK = fixed_bytes::deserialize_from(&mut key_cursor)?;
                let _namespace: N = fixed_bytes::deserialize_from(&mut key_cursor)?;
                let key = protobuf::deserialize_from(&mut key_cursor)?;

                Ok(key)
            });

        Ok(Box::new(iter))
    }

    fn map_values<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, V>> {
        let backend = self.initialized()?;

        let prefix = handle.serialize_metakeys()?;
        let cf = backend.get_cf_handle(handle.id)?;

        let iter = backend
            .db
            .prefix_iterator_cf(cf, prefix)?
            .map(move |(_, serialized_value)| {
                let value: V = protobuf::deserialize(&serialized_value)?;
                Ok(value)
            });

        Ok(Box::new(iter))
    }

    fn map_len<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<usize> {
        let backend = self.initialized()?;

        let prefix = handle.serialize_metakeys()?;
        let cf = backend.get_cf_handle(handle.id)?;

        let count = backend.db.prefix_iterator_cf(cf, prefix)?.count();

        Ok(count)
    }

    fn map_is_empty<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<bool> {
        let backend = self.initialized()?;

        let prefix = handle.serialize_metakeys()?;
        let cf = backend.get_cf_handle(handle.id)?;
        Ok(backend.db.prefix_iterator_cf(cf, prefix)?.next().is_none())
    }
}
