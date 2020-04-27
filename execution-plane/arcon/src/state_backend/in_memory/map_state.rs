// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::ArconResult,
    state_backend::{
        in_memory::{InMemory, StateCommon},
        serialization::{DeserializableWith, SerializableFixedSizeWith, SerializableWith},
        state_types::{BoxedIteratorOfArconResult, MapState, State},
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
        backend.remove_matching(&prefix);
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
    fn get(&self, backend: &InMemory, key: &K) -> ArconResult<Option<V>> {
        let key = self.common.get_db_key_with_user_key(key)?;
        if let Some(dynamic) = backend.get(&key) {
            let value: V = dynamic
                .downcast_ref::<V>()
                .ok_or_else(|| arcon_err_kind!("Dynamic value has a wrong type!"))?
                .clone();

            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn fast_insert(&self, backend: &mut InMemory, key: K, value: V) -> ArconResult<()> {
        let key = self.common.get_db_key_with_user_key(&key)?;
        let dynamic = SmallBox::new(value);
        let _old_value = backend.insert(key, dynamic);

        Ok(())
    }

    fn insert(&self, backend: &mut InMemory, key: K, value: V) -> ArconResult<Option<V>> {
        let key = self.common.get_db_key_with_user_key(&key)?;

        let new_dynamic = SmallBox::new(value);
        let old = match backend.insert(key, new_dynamic) {
            None => None,
            Some(old_dynamic) => Some(
                old_dynamic
                    .downcast_ref::<V>()
                    .ok_or_else(|| arcon_err_kind!("Dynamic value has a wrong type!"))?
                    .clone(),
            ),
        };

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
        let _old_value = backend.remove(&key);

        Ok(())
    }

    fn contains(&self, backend: &InMemory, key: &K) -> ArconResult<bool> {
        let key = self.common.get_db_key_with_user_key(key)?;
        Ok(backend.contains(&key))
    }

    // TODO: unboxed versions of below
    fn iter<'a>(
        &self,
        backend: &'a InMemory,
    ) -> ArconResult<BoxedIteratorOfArconResult<'a, (K, V)>> {
        let prefix = self.common.get_db_key_prefix()?;
        let id_len = self.common.id.as_bytes().len();
        let key_serializer = self.common.key_serializer.clone();
        let iter = backend.iter_matching(prefix).map(move |(k, v)| {
            let mut cursor = &k[id_len..];
            let _item_key = IK::deserialize_from(&key_serializer, &mut cursor)?;
            let _namespace = N::deserialize_from(&key_serializer, &mut cursor)?;
            let key = K::deserialize_from(&key_serializer, &mut cursor)?;
            let value = v
                .downcast_ref::<V>()
                .ok_or_else(|| arcon_err_kind!("Dynamic value has a wrong type!"))?
                .clone();
            Ok((key, value))
        });

        Ok(Box::new(iter))
    }

    fn keys<'a>(&self, backend: &'a InMemory) -> ArconResult<BoxedIteratorOfArconResult<'a, K>> {
        let prefix = self.common.get_db_key_prefix()?;
        let id_len = self.common.id.as_bytes().len();
        let key_serializer = self.common.key_serializer.clone();
        let iter = backend.iter_matching(prefix).map(move |(k, _)| {
            let mut cursor = &k[id_len..];
            let _ = IK::deserialize_from(&key_serializer, &mut cursor)?;
            let _ = N::deserialize_from(&key_serializer, &mut cursor)?;
            let key = K::deserialize_from(&key_serializer, &mut cursor)?;
            Ok(key)
        });

        Ok(Box::new(iter))
    }

    fn values<'a>(&self, backend: &'a InMemory) -> ArconResult<BoxedIteratorOfArconResult<'a, V>> {
        let prefix = self.common.get_db_key_prefix()?;
        let iter = backend.iter_matching(prefix).map(move |(_, v)| {
            let value = v
                .downcast_ref::<V>()
                .ok_or_else(|| arcon_err_kind!("Dynamic value has a wrong type!"))?
                .clone();
            Ok(value)
        });

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
    use crate::state_backend::{
        builders::MapStateBuilder,
        serialization::{NativeEndianBytesDump, Prost},
        StateBackend,
    };

    #[test]
    fn map_state_test() {
        let mut db = InMemory::new("test".as_ref()).unwrap();
        let map_state = db.new_map_state("test_state", (), (), NativeEndianBytesDump, Prost);

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

        assert_eq!(
            map_state
                .get(&db, &"first key".to_string())
                .unwrap()
                .unwrap(),
            42
        );
        assert_eq!(
            map_state
                .get(&db, &"second key".to_string())
                .unwrap()
                .unwrap(),
            69
        );

        assert_eq!(map_state.len(&db).unwrap(), 2);

        let keys: Vec<_> = map_state.keys(&db).unwrap().map(Result::unwrap).collect();

        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"first key".to_string()));
        assert!(keys.contains(&"second key".to_string()));

        map_state.clear(&mut db).unwrap();
        assert_eq!(map_state.len(&db).unwrap(), 0);
        assert!(map_state.is_empty(&db).unwrap());
    }
}
