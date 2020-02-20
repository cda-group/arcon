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
use smallbox::SmallBox;
use std::marker::PhantomData;

pub struct InMemoryMapState<IK, N, K, V, KS> {
    pub(crate) common: StateCommon<IK, N, KS>,
    // TODO: my phantom datas may be of wrong variance w.r.t. the lifetimes -- investigate
    pub(crate) _phantom: PhantomData<(K, V)>,
}

impl<IK, N, K, V, KS> State<InMemory, IK, N> for InMemoryMapState<IK, N, K, V, KS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
{
    fn clear(&self, backend: &mut InMemory) -> ArconResult<()> {
        let prefix = self.common.get_db_key_prefix()?;
        backend.remove_matching(&prefix)?;
        Ok(())
    }

    delegate_key_and_namespace!(common);
}

impl<IK, N, K, V, KS> MapState<InMemory, IK, N, K, V> for InMemoryMapState<IK, N, K, V, KS>
where
    IK: SerializableFixedSizeWith<KS> + DeserializableWith<KS>,
    N: SerializableFixedSizeWith<KS> + DeserializableWith<KS>,
    K: SerializableWith<KS> + DeserializableWith<KS>,
    V: Send + Sync + Clone + 'static,
    KS: Clone + 'static,
{
    fn get(&self, backend: &InMemory, key: &K) -> ArconResult<V> {
        let key = self.common.get_db_key_with_user_key(key)?;
        let dynamic = backend.get(&key)?;
        let value = dynamic
            .downcast_ref::<V>()
            .ok_or_else(|| arcon_err_kind!("Dynamic value has a wrong type!"))?
            .clone();

        Ok(value)
    }

    fn fast_insert(&self, backend: &mut InMemory, key: K, value: V) -> ArconResult<()> {
        let key = self.common.get_db_key_with_user_key(&key)?;
        let dynamic = SmallBox::new(value);
        backend.put(key, dynamic)?;

        Ok(())
    }

    fn insert(&self, backend: &mut InMemory, key: K, value: V) -> ArconResult<Option<V>> {
        let key = self.common.get_db_key_with_user_key(&key)?;

        let old = match backend.get_mut(&key) {
            // TODO: only handle value not found
            Err(_) => None,
            Ok(dynamic) => Some(
                dynamic
                    .downcast_ref::<V>()
                    .ok_or_else(|| arcon_err_kind!("Dynamic value has a wrong type!"))?
                    .clone(),
            ),
        };

        let dynamic = SmallBox::new(value);
        backend.put(key, dynamic)?;

        Ok(old)
    }

    fn insert_all_dyn(
        &self,
        backend: &mut InMemory,
        key_value_pairs: &mut dyn Iterator<Item = (K, V)>,
    ) -> ArconResult<()> {
        self.insert_all(backend, key_value_pairs)
    }

    fn insert_all(
        &self,
        backend: &mut InMemory,
        key_value_pairs: impl IntoIterator<Item = (K, V)>,
    ) -> ArconResult<()>
    where
        Self: Sized,
    {
        for (k, v) in key_value_pairs.into_iter() {
            self.fast_insert(backend, k, v)?; // TODO: what if one fails? partial insert? should we roll back?
        }

        Ok(())
    }

    fn remove(&self, backend: &mut InMemory, key: &K) -> ArconResult<()> {
        let key = self.common.get_db_key_with_user_key(key)?;
        backend.remove(&key)?;

        Ok(())
    }

    fn contains(&self, backend: &InMemory, key: &K) -> ArconResult<bool> {
        let key = self.common.get_db_key_with_user_key(key)?;
        backend.contains(&key)
    }

    // TODO: unboxed versions of below
    fn iter<'a>(
        &self,
        backend: &'a InMemory,
    ) -> ArconResult<Box<dyn Iterator<Item = (K, V)> + 'a>> {
        let prefix = self.common.get_db_key_prefix()?;
        let id_len = self.common.id.as_bytes().len();
        let key_serializer = self.common.key_serializer.clone();
        let iter = backend
            .iter_matching(prefix)
            .map(move |(k, v)| {
                let mut cursor = &k[id_len..];
                let _ = IK::deserialize_from(&key_serializer, &mut cursor)?;
                let _ = N::deserialize_from(&key_serializer, &mut cursor)?;
                let key = K::deserialize_from(&key_serializer, &mut cursor)?;
                let value = v
                    .downcast_ref::<V>()
                    .ok_or_else(|| arcon_err_kind!("Dynamic value has a wrong type!"))?
                    .clone();
                Ok((key, value))
            })
            .map(|res: ArconResult<(K, V)>| res.expect("deserialization error"));
        // TODO: we panic above if deserialization fails. Perhaps the function signature should
        //  change to accommodate for that

        Ok(Box::new(iter))
    }

    fn keys<'a>(&self, backend: &'a InMemory) -> ArconResult<Box<dyn Iterator<Item = K> + 'a>> {
        let prefix = self.common.get_db_key_prefix()?;
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
        let prefix = self.common.get_db_key_prefix()?;
        let iter = backend
            .iter_matching(prefix)
            .map(move |(_, v)| {
                let value = v
                    .downcast_ref::<V>()
                    .ok_or_else(|| arcon_err_kind!("Dynamic value has a wrong type!"))?
                    .clone();
                Ok(value)
            })
            .map(|res: ArconResult<V>| res.expect("deserialization error"));
        // TODO: we panic above if deserialization fails. Perhaps the function signature should
        //  change to accommodate for that

        Ok(Box::new(iter))
    }

    fn len(&self, backend: &InMemory) -> ArconResult<usize> {
        let prefix = self.common.get_db_key_prefix()?;
        let count = backend.iter_matching(prefix).count();
        Ok(count)
    }

    fn is_empty(&self, backend: &InMemory) -> ArconResult<bool> {
        let prefix = self.common.get_db_key_prefix()?;
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

        map_state
            .fast_insert(&mut db, "first key".to_string(), 42)
            .unwrap();
        map_state
            .fast_insert(&mut db, "second key".to_string(), 69)
            .unwrap();

        assert!(map_state.contains(&db, &"first key".to_string()).unwrap());
        assert!(map_state.contains(&db, &"second key".to_string()).unwrap());

        assert_eq!(map_state.get(&db, &"first key".to_string()).unwrap(), 42);
        assert_eq!(map_state.get(&db, &"second key".to_string()).unwrap(), 69);

        assert_eq!(map_state.len(&db).unwrap(), 2);

        let keys: Vec<_> = map_state.keys(&db).unwrap().collect();

        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"first key".to_string()));
        assert!(keys.contains(&"second key".to_string()));

        map_state.clear(&mut db).unwrap();
        assert_eq!(map_state.len(&db).unwrap(), 0);
        assert!(map_state.is_empty(&db).unwrap());
    }
}
