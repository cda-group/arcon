// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
use crate::{
    error::*,
    handles::BoxedIteratorOfResult,
    serialization::{fixed_bytes, protobuf},
    Handle, InMemory, Key, MapOps, MapState, Metakey, Value,
};
use smallbox::SmallBox;
use std::any::Any;

impl MapOps for InMemory {
    fn map_clear<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;
        self.get_mut(handle)
            .retain(|k, _| &k[..key.len()] != &key[..]);
        Ok(())
    }

    fn map_get<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<Option<V>> {
        let key = handle.serialize_metakeys_and_key(key)?;
        if let Some(dynamic) = self.get(handle).get(&key) {
            let value = dynamic
                .downcast_ref::<V>()
                .context(InMemoryWrongType)?
                .clone();

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
        let dynamic = SmallBox::new(value);
        let _old_value = self.get_mut(handle).insert(key, dynamic);

        Ok(())
    }

    fn map_insert<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: K,
        value: V,
    ) -> Result<Option<V>> {
        let key = handle.serialize_metakeys_and_key(&key)?;

        let new_dynamic = SmallBox::new(value);
        let old = match self.get_mut(handle).insert(key, new_dynamic) {
            None => None,
            Some(old_dynamic) => Some(
                old_dynamic
                    .downcast_ref::<V>()
                    .context(InMemoryWrongType)?
                    .clone(),
            ),
        };

        Ok(old)
    }

    fn map_insert_all<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key_value_pairs: impl IntoIterator<Item = (K, V)>,
    ) -> Result<()> {
        for (k, v) in key_value_pairs.into_iter() {
            self.map_fast_insert(handle, k, v)?; // TODO: what if one fails? partial insert? should we roll back?
        }

        Ok(())
    }

    fn map_remove<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<Option<V>> {
        let key = handle.serialize_metakeys_and_key(key)?;
        let old_value = self.get_mut(handle).remove(&key);
        if let Some(dynamic) = old_value {
            let dynamic = dynamic as SmallBox<dyn Any, _>;
            let typed = dynamic.downcast::<V>().ok().context(InMemoryWrongType)?;
            Ok(Some(typed.into_inner()))
        } else {
            Ok(None)
        }
    }

    fn map_fast_remove<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<()> {
        let key = handle.serialize_metakeys_and_key(key)?;
        let _old_value = self.get_mut(handle).remove(&key);

        Ok(())
    }

    fn map_contains<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<bool> {
        let key = handle.serialize_metakeys_and_key(key)?;
        Ok(self.get(handle).contains_key(&key))
    }

    fn map_iter<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, (K, V)>> {
        let prefix = handle.serialize_metakeys()?;
        let iter = self
            .get(handle)
            .iter()
            .filter_map(move |(k, v)| {
                if &k[..prefix.len()] != &prefix[..] {
                    return None;
                }

                Some((k.as_slice(), &**v))
            })
            .map(move |(k, v)| {
                let mut cursor = k;
                let _item_key: IK = fixed_bytes::deserialize_from(&mut cursor)?;
                let _namespace: N = fixed_bytes::deserialize_from(&mut cursor)?;
                let key: K = protobuf::deserialize_from(&mut cursor)?;
                let value = v.downcast_ref::<V>().context(InMemoryWrongType)?.clone();
                Ok((key, value))
            });

        Ok(Box::new(iter))
    }

    fn map_keys<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, K>> {
        let prefix = handle.serialize_metakeys()?;
        let iter = self
            .get(handle)
            .iter()
            .filter_map(move |(k, _v)| {
                if &k[..prefix.len()] != &prefix[..] {
                    return None;
                }

                Some(k.as_slice())
            })
            .map(move |k| {
                let mut cursor = k;
                let _item_key: IK = fixed_bytes::deserialize_from(&mut cursor)?;
                let _namespace: N = fixed_bytes::deserialize_from(&mut cursor)?;
                let key: K = protobuf::deserialize_from(&mut cursor)?;
                Ok(key)
            });

        Ok(Box::new(iter))
    }

    fn map_values<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, V>> {
        let prefix = handle.serialize_metakeys()?;
        let iter = self
            .get(handle)
            .iter()
            .filter_map(move |(k, v)| {
                if &k[..prefix.len()] != &prefix[..] {
                    return None;
                }

                Some(&**v)
            })
            .map(move |v| {
                let value = v.downcast_ref::<V>().context(InMemoryWrongType)?.clone();
                Ok(value)
            });

        Ok(Box::new(iter))
    }

    fn map_len<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<usize> {
        let prefix = handle.serialize_metakeys()?;
        let count = self
            .get(handle)
            .iter()
            .filter(move |(k, _v)| &k[..prefix.len()] == &prefix[..])
            .count();
        Ok(count)
    }

    fn map_is_empty<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<bool> {
        let prefix = handle.serialize_metakeys()?;
        let is_empty = self
            .get(handle)
            .iter()
            .filter(move |(k, _v)| &k[..prefix.len()] == &prefix[..])
            .next()
            .is_none();
        Ok(is_empty)
    }
}
