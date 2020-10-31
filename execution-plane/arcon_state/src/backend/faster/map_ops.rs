// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
use crate::{
    error::*,
    handles::BoxedIteratorOfResult,
    serialization::{fixed_bytes, protobuf},
    Faster, Handle, Key, MapOps, MapState, Metakey, Value,
};

impl MapOps for Faster {
    fn map_clear<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<()> {
        let prefix = handle.serialize_id_and_metakeys()?;
        let keys = self.get_vec(&prefix)?;
        let keys = if let Some(k) = keys { k } else { return Ok(()) };
        self.remove(&prefix)?;
        for key in keys {
            self.remove(&key)?;
        }
        Ok(())
    }

    fn map_get<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<Option<V>> {
        let key = handle.serialize_id_metakeys_and_key(key)?;
        if let Some(serialized) = self.get(&key)? {
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
        let prefix = handle.serialize_id_and_metakeys()?;
        let key = handle.serialize_id_metakeys_and_key(&key)?;
        let serialized = protobuf::serialize(&value)?;
        self.put(&key, &serialized)?;
        self.vec_push_if_absent(&prefix, key)?;

        Ok(())
    }

    fn map_insert<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: K,
        value: V,
    ) -> Result<Option<V>> {
        let prefix = handle.serialize_id_and_metakeys()?;
        let key = handle.serialize_id_metakeys_and_key(&key)?;

        let old = if let Some(slice) = self.get(&key)? {
            Some(protobuf::deserialize(&*slice)?)
        } else {
            None
        };

        let serialized = protobuf::serialize(&value)?;
        self.put(&key, &serialized)?;
        self.vec_push_if_absent(&prefix, key)?;

        Ok(old)
    }

    fn map_fast_insert_by_ref<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
        value: &V,
    ) -> Result<()> {
        let prefix = handle.serialize_id_and_metakeys()?;
        let key = handle.serialize_id_metakeys_and_key(key)?;
        let serialized = protobuf::serialize(value)?;
        self.put(&key, &serialized)?;
        self.vec_push_if_absent(&prefix, key)?;

        Ok(())
    }

    fn map_insert_all<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key_value_pairs: impl IntoIterator<Item = (K, V)>,
    ) -> Result<()> {
        let prefix = handle.serialize_id_and_metakeys()?;
        for (user_key, value) in key_value_pairs {
            let key = handle.serialize_id_metakeys_and_key(&user_key)?;
            let serialized = protobuf::serialize(&value)?;
            self.put(&key, &serialized)?;
            self.vec_push_if_absent(&prefix, key)?;
        }

        Ok(())
    }
    fn map_insert_all_by_ref<'a, K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key_value_pairs: impl IntoIterator<Item = (&'a K, &'a V)>,
    ) -> Result<()> {
        let prefix = handle.serialize_id_and_metakeys()?;
        for (user_key, value) in key_value_pairs {
            let key = handle.serialize_id_metakeys_and_key(user_key)?;
            let serialized = protobuf::serialize(value)?;
            self.put(&key, &serialized)?;
            self.vec_push_if_absent(&prefix, key)?;
        }
        Ok(())
    }

    fn map_remove<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<Option<V>> {
        let prefix = handle.serialize_id_and_metakeys()?;
        let key = handle.serialize_id_metakeys_and_key(key)?;

        let old = if let Some(slice) = self.get(&key)? {
            Some(protobuf::deserialize(&*slice)?)
        } else {
            None
        };

        self.remove(&key)?;
        self.vec_remove(&prefix, key)?;

        Ok(old)
    }

    fn map_fast_remove<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<()> {
        let prefix = handle.serialize_id_and_metakeys()?;
        let key = handle.serialize_id_metakeys_and_key(key)?;
        self.remove(&key)?;
        self.vec_remove(&prefix, key)?;

        Ok(())
    }

    fn map_contains<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<bool> {
        let prefix = handle.serialize_id_and_metakeys()?;
        let key = handle.serialize_id_metakeys_and_key(key)?;
        let keys = self.get_vec(&prefix)?;
        if let Some(keys) = keys {
            Ok(keys.contains(&key))
        } else {
            Ok(false)
        }
    }

    fn map_iter<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, (K, V)>> {
        let prefix = handle.serialize_id_and_metakeys()?;
        let keys = self.get_vec(&prefix)?;

        let iter = keys.into_iter().flatten().map(move |serialized_key| {
            let serialized_value = self
                .get(&serialized_key)?
                .expect("Value not found. Modified during iteration!?");

            let mut key_cursor = &serialized_key[..];
            let _ = fixed_bytes::deserialize_bytes_from(&mut key_cursor)?;
            let _: IK = fixed_bytes::deserialize_from(&mut key_cursor)?;
            let _: N = fixed_bytes::deserialize_from(&mut key_cursor)?;
            let key: K = protobuf::deserialize_from(&mut key_cursor)?;
            let value = protobuf::deserialize(&serialized_value)?;
            Ok((key, value))
        });

        Ok(Box::new(iter))
    }

    fn map_keys<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, K>> {
        let prefix = handle.serialize_id_and_metakeys()?;
        let keys = self.get_vec(&prefix)?;

        let iter = keys.into_iter().flatten().map(move |serialized_key| {
            let mut key_cursor = &serialized_key[..];
            let _ = fixed_bytes::deserialize_bytes_from(&mut key_cursor)?;
            let _: IK = fixed_bytes::deserialize_from(&mut key_cursor)?;
            let _: N = fixed_bytes::deserialize_from(&mut key_cursor)?;
            let key: K = protobuf::deserialize_from(&mut key_cursor)?;
            Ok(key)
        });

        Ok(Box::new(iter))
    }

    fn map_values<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, V>> {
        let prefix = handle.serialize_id_and_metakeys()?;
        let keys = self.get_vec(&prefix)?;

        let iter = keys.into_iter().flatten().map(move |serialized_key| {
            let serialized_value = self
                .get(&serialized_key)?
                .expect("Value not found. Modified during iteration!?");

            let value = protobuf::deserialize(&serialized_value)?;
            Ok(value)
        });

        Ok(Box::new(iter))
    }

    fn map_len<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<usize> {
        let prefix = handle.serialize_id_and_metakeys()?;
        let keys = self.get_vec(&prefix)?;
        Ok(keys.map(|keys| keys.len()).unwrap_or(0))
    }

    fn map_is_empty<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<bool> {
        let prefix = handle.serialize_id_and_metakeys()?;
        let keys = self.get_vec(&prefix)?;
        Ok(keys.map(|keys| keys.is_empty()).unwrap_or(true))
    }
}
