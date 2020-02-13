// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::ArconResult,
    state_backend::{
        in_memory::{InMemory, StateCommon},
        serialization::{DeserializableWith, SerializableFixedSizeWith, SerializableWith},
        state_types::{MapState, State},
    },
};
use std::marker::PhantomData;

pub struct InMemoryMapState<IK, N, K, V, KS, TS> {
    pub(crate) common: StateCommon<IK, N, KS, TS>,
    // TODO: my phantom datas may be of wrong variance w.r.t. the lifetimes -- investigate
    pub(crate) _phantom: PhantomData<(K, V)>,
}

impl<IK, N, K, V, KS, TS> State<InMemory, IK, N> for InMemoryMapState<IK, N, K, V, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    (): SerializableWith<KS>,
{
    fn clear(&self, backend: &mut InMemory) -> ArconResult<()> {
        // () is not serialized, and the user key is the tail of the db key, so in effect we get
        // the prefix with which to search the underlying db.
        let prefix = self.common.get_db_key(&())?;
        backend.remove_matching(&prefix)?;
        Ok(())
    }

    delegate_key_and_namespace!(common);
}

impl<IK, N, K, V, KS, TS> MapState<InMemory, IK, N, K, V> for InMemoryMapState<IK, N, K, V, KS, TS>
where
    IK: SerializableFixedSizeWith<KS> + DeserializableWith<KS>,
    N: SerializableFixedSizeWith<KS> + DeserializableWith<KS>,
    (): SerializableWith<KS>,
    K: SerializableWith<KS> + DeserializableWith<KS>,
    V: SerializableWith<TS> + DeserializableWith<TS>,
    KS: Clone + 'static,
    TS: Clone + 'static,
{
    fn get(&self, backend: &InMemory, key: &K) -> ArconResult<V> {
        let key = self.common.get_db_key(key)?;
        let serialized = backend.get(&key)?;
        let value = V::deserialize(&self.common.value_serializer, &serialized)?;

        Ok(value)
    }

    fn put(&self, backend: &mut InMemory, key: K, value: V) -> ArconResult<()> {
        let key = self.common.get_db_key(&key)?;
        let serialized = V::serialize(&self.common.value_serializer, &value)?;
        backend.put(key, serialized)?;

        Ok(())
    }

    fn put_all_dyn(
        &self,
        backend: &mut InMemory,
        key_value_pairs: &mut dyn Iterator<Item = (K, V)>,
    ) -> ArconResult<()> {
        self.put_all(backend, key_value_pairs)
    }

    fn put_all(
        &self,
        backend: &mut InMemory,
        key_value_pairs: impl IntoIterator<Item = (K, V)>,
    ) -> ArconResult<()>
    where
        Self: Sized,
    {
        for (k, v) in key_value_pairs.into_iter() {
            self.put(backend, k, v)?; // TODO: what if one fails? partial insert? should we roll back?
        }

        Ok(())
    }

    fn remove(&self, backend: &mut InMemory, key: &K) -> ArconResult<()> {
        let key = self.common.get_db_key(key)?;
        backend.remove(&key)?;

        Ok(())
    }

    fn contains(&self, backend: &InMemory, key: &K) -> ArconResult<bool> {
        let key = self.common.get_db_key(key)?;
        backend.contains(&key)
    }

    // TODO: unboxed versions of below
    fn iter<'a>(
        &self,
        backend: &'a InMemory,
    ) -> ArconResult<Box<dyn Iterator<Item = (K, V)> + 'a>> {
        let prefix = self.common.get_db_key(&())?;
        let id_len = self.common.id.as_bytes().len();
        let key_serializer = self.common.key_serializer.clone();
        let value_serializer = self.common.value_serializer.clone();
        let iter = backend
            .iter_matching(prefix)
            .map(move |(k, v)| {
                let mut cursor = &k[id_len..];
                let _ = IK::deserialize_from(&key_serializer, &mut cursor)?;
                let _ = N::deserialize_from(&key_serializer, &mut cursor)?;
                let key = K::deserialize_from(&key_serializer, &mut cursor)?;
                let value = V::deserialize(&value_serializer, v)?;
                Ok((key, value))
            })
            .map(|res: ArconResult<(K, V)>| res.expect("deserialization error"));
        // TODO: we panic above if deserialization fails. Perhaps the function signature should
        //  change to accommodate for that

        Ok(Box::new(iter))
    }

    fn keys<'a>(&self, backend: &'a InMemory) -> ArconResult<Box<dyn Iterator<Item = K> + 'a>> {
        let prefix = self.common.get_db_key(&())?;
        let id_len = self.common.id.as_bytes().len();
        let key_serializer = self.common.key_serializer.clone();
        let iter = backend
            .iter_matching(prefix)
            .map(move |(k, _)| {
                let mut cursor = &k[id_len..];
                let _ = IK::deserialize_from(&key_serializer, &mut cursor)?;
                let _ = N::deserialize_from(&key_serializer, &mut cursor)?;
                let key = K::deserialize_from(&key_serializer, &mut cursor)?;
                Ok(key)
            })
            .map(|res: ArconResult<K>| res.expect("deserialization error"));
        // TODO: we panic above if deserialization fails. Perhaps the function signature should
        //  change to accommodate for that

        Ok(Box::new(iter))
    }

    fn values<'a>(&self, backend: &'a InMemory) -> ArconResult<Box<dyn Iterator<Item = V> + 'a>> {
        let prefix = self.common.get_db_key(&())?;
        let value_serializer = self.common.value_serializer.clone();
        let iter = backend
            .iter_matching(prefix)
            .map(move |(_, v)| {
                let value = V::deserialize(&value_serializer, v)?;
                Ok(value)
            })
            .map(|res: ArconResult<V>| res.expect("deserialization error"));
        // TODO: we panic above if deserialization fails. Perhaps the function signature should
        //  change to accommodate for that

        Ok(Box::new(iter))
    }

    fn is_empty(&self, backend: &InMemory) -> ArconResult<bool> {
        let prefix = self.common.get_db_key(&())?;
        Ok(backend.iter_matching(prefix).next().is_none())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::{serialization::Bincode, MapStateBuilder, StateBackend};

    #[test]
    fn map_state_test() {
        let mut db = InMemory::new("test").unwrap();
        let map_state = db.new_map_state("test_state", (), (), Bincode, Bincode);

        // TODO: &String is weird, maybe look at how it's done with the keys in std hash-map
        assert!(!map_state.contains(&db, &"first key".to_string()).unwrap());

        map_state.put(&mut db, "first key".to_string(), 42).unwrap();
        map_state
            .put(&mut db, "second key".to_string(), 69)
            .unwrap();

        assert!(map_state.contains(&db, &"first key".to_string()).unwrap());
        assert!(map_state.contains(&db, &"second key".to_string()).unwrap());

        assert_eq!(map_state.get(&db, &"first key".to_string()).unwrap(), 42);
        assert_eq!(map_state.get(&db, &"second key".to_string()).unwrap(), 69);

        let keys: Vec<_> = map_state.keys(&db).unwrap().collect();

        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"first key".to_string()));
        assert!(keys.contains(&"second key".to_string()));
    }
}
