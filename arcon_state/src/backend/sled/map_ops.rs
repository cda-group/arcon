// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
use crate::{
    data::{Key, Metakey, Value},
    error::*,
    handles::BoxedIteratorOfResult,
    serialization::{fixed_bytes, protobuf},
    sled::Sled,
    Handle, MapOps, MapState,
};
use sled::Batch;

impl MapOps for Sled {
    fn map_clear<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<()> {
        let prefix = handle.serialize_metakeys()?;
        self.remove_prefix(&handle.id, prefix)
    }

    fn map_get<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<Option<V>> {
        let key = handle.serialize_metakeys_and_key(key)?;
        if let Some(serialized) = self.get(&handle.id, &key)? {
            gauge!(format!("{}_bytes_read", handle.get_name()), serialized.len() as f64, "backend" => self.name.clone());
            let value = protobuf::deserialize(&serialized)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn map_fast_insert<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: K,
        value: V,
    ) -> Result<()> {
        let key = handle.serialize_metakeys_and_key(&key)?;
        let serialized = protobuf::serialize(&value)?;
        gauge!(format!("{}_bytes_written", handle.get_name()), serialized.len() as f64, "backend" => self.name.clone());
        self.put(&handle.id, &key, &serialized)?;

        Ok(())
    }

    fn map_fast_insert_by_ref<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
        value: &V,
    ) -> Result<()> {
        let key = handle.serialize_metakeys_and_key(key)?;
        let serialized = protobuf::serialize(value)?;
        gauge!(format!("{}_bytes_written", handle.get_name()), serialized.len() as f64, "backend" => self.name.clone());
        self.put(&handle.id, &key, &serialized)?;

        Ok(())
    }

    fn map_insert<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: K,
        value: V,
    ) -> Result<Option<V>> {
        let key = handle.serialize_metakeys_and_key(&key)?;

        let serialized = protobuf::serialize(&value)?;
        let old = match self.put(&handle.id, &key, &serialized)? {
            gauge!(format!("{}_bytes_written", handle.get_name()), slice.len() as f64, "backend" => self.name.clone());
            Some(x) => Some(protobuf::deserialize(&x)?),
            None => None,
        };

        Ok(old)
    }

    fn map_insert_all<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key_value_pairs: impl IntoIterator<Item = (K, V)>,
    ) -> Result<()> {
        let mut batch = Batch::default();
        let tree = self.tree(&handle.id)?;

        for (user_key, value) in key_value_pairs {
            let key = handle.serialize_metakeys_and_key(&user_key)?;
            let serialized = protobuf::serialize(&value)?;
            gauge!(format!("{}_bytes_written", handle.get_name()), serialized.len() as f64, "backend" => self.name.clone());
            batch.insert(key, serialized);
        }

        Ok(tree.apply_batch(batch)?)
    }

    fn map_insert_all_by_ref<'a, K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key_value_pairs: impl IntoIterator<Item = (&'a K, &'a V)>,
    ) -> Result<()> {
        let mut batch = Batch::default();
        let tree = self.tree(&handle.id)?;

        for (user_key, value) in key_value_pairs {
            let key = handle.serialize_metakeys_and_key(user_key)?;
            let serialized = protobuf::serialize(value)?;
            gauge!(format!("{}_bytes_written", handle.get_name()), serialized.len() as f64, "backend" => self.name.clone());
            batch.insert(key, serialized);
        }

        Ok(tree.apply_batch(batch)?)
    }

    fn map_remove<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<Option<V>> {
        let key = handle.serialize_metakeys_and_key(key)?;
        let old = match self.remove(&handle.id, &key)? {
            Some(bytes) => Some(protobuf::deserialize(bytes.as_ref())?),
            None => None,
        };

        Ok(old)
    }

    fn map_fast_remove<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<()> {
        let key = handle.serialize_metakeys_and_key(key)?;
        self.remove(&handle.id, &key)?;

        Ok(())
    }

    fn map_contains<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<bool> {
        let key = handle.serialize_metakeys_and_key(key)?;
        self.contains(&handle.id, &key)
    }

    fn map_iter<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, (K, V)>> {
        let prefix = handle.serialize_metakeys()?;
        let tree = self.tree(&handle.id)?;

        let iter = tree.scan_prefix(prefix).map(move |entry| {
            let (db_key, serialized_value) = entry?;
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
        let prefix = handle.serialize_metakeys()?;
        let tree = self.tree(&handle.id)?;

        let iter = tree.scan_prefix(prefix).map(move |entry| {
            let (db_key, _) = entry?;
            let mut key_cursor = &db_key[..];
            let _item_key: IK = fixed_bytes::deserialize_from(&mut key_cursor)?;
            let _namespace: N = fixed_bytes::deserialize_from(&mut key_cursor)?;
            let key: K = protobuf::deserialize_from(&mut key_cursor)?;

            Ok(key)
        });

        Ok(Box::new(iter))
    }

    fn map_values<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, V>> {
        let prefix = handle.serialize_metakeys()?;
        let tree = self.tree(&handle.id)?;

        let iter = tree.scan_prefix(prefix).map(move |entry| {
            let (_, serialized_value) = entry?;
            let value: V = protobuf::deserialize(&serialized_value)?;

            Ok(value)
        });

        Ok(Box::new(iter))
    }

    fn map_len<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<usize> {
        let prefix = handle.serialize_metakeys()?;
        let tree = self.tree(&handle.id)?;
        let count = tree.scan_prefix(prefix).count();

        Ok(count)
    }

    fn map_is_empty<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<bool> {
        let prefix = handle.serialize_metakeys()?;
        let tree = self.tree(&handle.id)?;
        Ok(tree.scan_prefix(prefix).next().is_none())
    }
}
