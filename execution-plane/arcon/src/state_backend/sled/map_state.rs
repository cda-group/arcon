// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::ArconResult,
    state_backend::{
        serialization::{DeserializableWith, SerializableFixedSizeWith, SerializableWith},
        sled::{Sled, StateCommon},
        state_types::{BoxedIteratorOfArconResult, MapState, State},
    },
};
use error::ResultExt;
use sled::Batch;
use std::marker::PhantomData;

pub struct SledMapState<IK, N, K, V, KS, TS> {
    pub(crate) common: StateCommon<IK, N, KS, TS>,
    pub(crate) _phantom: PhantomData<(K, V)>,
}

impl<IK, N, K, V, KS, TS> State<Sled, IK, N> for SledMapState<IK, N, K, V, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
{
    fn clear(&self, backend: &mut Sled) -> ArconResult<()> {
        let prefix = self.common.get_db_key_prefix()?;
        backend.remove_prefix(&self.common.tree_name, prefix)
    }

    delegate_key_and_namespace!(common);
}

impl<IK, N, K, V, KS, TS> MapState<Sled, IK, N, K, V> for SledMapState<IK, N, K, V, KS, TS>
where
    IK: SerializableFixedSizeWith<KS> + DeserializableWith<KS>,
    N: SerializableFixedSizeWith<KS> + DeserializableWith<KS>,
    K: SerializableWith<KS> + DeserializableWith<KS>,
    V: SerializableWith<TS> + DeserializableWith<TS>,
    KS: Clone + 'static,
    TS: Clone + 'static,
{
    fn get(&self, backend: &Sled, key: &K) -> ArconResult<Option<V>> {
        let key = self.common.get_db_key_with_user_key(key)?;
        if let Some(serialized) = backend.get(&self.common.tree_name, &key)? {
            let value = V::deserialize(&self.common.value_serializer, &serialized)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn fast_insert(&self, backend: &mut Sled, key: K, value: V) -> ArconResult<()> {
        let key = self.common.get_db_key_with_user_key(&key)?;
        let serialized = V::serialize(&self.common.value_serializer, &value)?;
        backend.put(&self.common.tree_name, &key, &serialized)?;

        Ok(())
    }

    fn insert(&self, backend: &mut Sled, key: K, value: V) -> ArconResult<Option<V>> {
        let key = self.common.get_db_key_with_user_key(&key)?;

        let serialized = V::serialize(&self.common.value_serializer, &value)?;
        let old = match backend.put(&self.common.tree_name, &key, &serialized)? {
            Some(x) => Some(V::deserialize(&self.common.value_serializer, &x)?),
            None => None,
        };

        Ok(old)
    }

    fn insert_all_dyn(
        &self,
        backend: &mut Sled,
        key_value_pairs: &mut dyn Iterator<Item = (K, V)>,
    ) -> ArconResult<()> {
        self.insert_all(backend, key_value_pairs)
    }

    fn insert_all(
        &self,
        backend: &mut Sled,
        key_value_pairs: impl IntoIterator<Item = (K, V)>,
    ) -> ArconResult<()>
    where
        Self: Sized,
    {
        let mut batch = Batch::default();
        let tree = backend.tree(&self.common.tree_name)?;

        for (user_key, value) in key_value_pairs {
            let key = self.common.get_db_key_with_user_key(&user_key)?;
            let serialized = V::serialize(&self.common.value_serializer, &value)?;
            batch.insert(key, serialized);
        }

        tree.apply_batch(batch)
            .ctx("Could not perform batch insert operation")
    }

    fn remove(&self, backend: &mut Sled, key: &K) -> ArconResult<()> {
        let key = self.common.get_db_key_with_user_key(key)?;
        backend.remove(&self.common.tree_name, &key)?;

        Ok(())
    }

    fn contains(&self, backend: &Sled, key: &K) -> ArconResult<bool> {
        let key = self.common.get_db_key_with_user_key(key)?;
        backend.contains(&self.common.tree_name, &key)
    }

    // TODO: unboxed versions of below
    fn iter<'a>(&self, backend: &'a Sled) -> ArconResult<BoxedIteratorOfArconResult<'a, (K, V)>> {
        let prefix = self.common.get_db_key_prefix()?;
        let tree = backend.tree(&self.common.tree_name)?;
        let key_serializer = self.common.key_serializer.clone();
        let value_serializer = self.common.value_serializer.clone();

        let iter = tree.scan_prefix(prefix).map(move |entry| {
            let (db_key, serialized_value) =
                entry.ctx("Could not get the values from sled iterator")?;
            let mut key_cursor = &db_key[..];
            let _item_key = IK::deserialize_from(&key_serializer, &mut key_cursor)?;
            let _namespace = N::deserialize_from(&key_serializer, &mut key_cursor)?;
            let key = K::deserialize_from(&key_serializer, &mut key_cursor)?;
            let value = V::deserialize(&value_serializer, &serialized_value)?;

            Ok((key, value))
        });

        Ok(Box::new(iter))
    }

    fn keys<'a>(&self, backend: &'a Sled) -> ArconResult<BoxedIteratorOfArconResult<'a, K>> {
        let prefix = self.common.get_db_key_prefix()?;
        let tree = backend.tree(&self.common.tree_name)?;
        let key_serializer = self.common.key_serializer.clone();

        let iter = tree.scan_prefix(prefix).map(move |entry| {
            let (db_key, _) = entry.ctx("Could not get the values from sled iterator")?;
            let mut key_cursor = &db_key[..];
            let _ = IK::deserialize_from(&key_serializer, &mut key_cursor)?;
            let _ = N::deserialize_from(&key_serializer, &mut key_cursor)?;
            let key = K::deserialize_from(&key_serializer, &mut key_cursor)?;

            Ok(key)
        });

        Ok(Box::new(iter))
    }

    fn values<'a>(&self, backend: &'a Sled) -> ArconResult<BoxedIteratorOfArconResult<'a, V>> {
        let prefix = self.common.get_db_key_prefix()?;
        let tree = backend.tree(&self.common.tree_name)?;
        let value_serializer = self.common.value_serializer.clone();

        let iter = tree.scan_prefix(prefix).map(move |entry| {
            let (_, serialized_value) = entry.ctx("Could not get the values from sled iterator")?;
            let value: V = V::deserialize(&value_serializer, &serialized_value)?;

            Ok(value)
        });

        Ok(Box::new(iter))
    }

    fn len(&self, backend: &Sled) -> ArconResult<usize> {
        let prefix = self.common.get_db_key_prefix()?;
        let tree = backend.tree(&self.common.tree_name)?;
        let count = tree.scan_prefix(prefix).count();

        Ok(count)
    }

    fn is_empty(&self, backend: &Sled) -> ArconResult<bool> {
        let prefix = self.common.get_db_key_prefix()?;
        let tree = backend.tree(&self.common.tree_name)?;
        Ok(tree.scan_prefix(prefix).next().is_none())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::{
        serialization::NativeEndianBytesDump, sled::test::TestDb, MapStateBuilder,
    };

    #[test]
    fn map_state_test() {
        let mut db = TestDb::new();
        let map_state = db.new_map_state(
            "test_state",
            (),
            (),
            NativeEndianBytesDump,
            NativeEndianBytesDump,
        );

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

    #[test]
    fn clearing_test() {
        let mut db = TestDb::new();
        let mut map_state = db.new_map_state(
            "test_state",
            0u8,
            0u8,
            NativeEndianBytesDump,
            NativeEndianBytesDump,
        );

        let mut expected_for_key_zero = vec![];
        for i in 0..10 {
            let key = i.to_string();
            let value = i;
            expected_for_key_zero.push((key.clone(), value));
            map_state.fast_insert(&mut db, key, value).unwrap()
        }

        map_state.set_current_key(1).unwrap();

        let mut expected_for_key_one = vec![];
        for i in 10..20 {
            let key = i.to_string();
            let value = i;
            expected_for_key_one.push((key.clone(), value));
            map_state.fast_insert(&mut db, key, value).unwrap()
        }

        let tuples_from_key_one: Vec<_> =
            map_state.iter(&db).unwrap().map(Result::unwrap).collect();
        assert_eq!(tuples_from_key_one, expected_for_key_one);

        map_state.set_current_key(0).unwrap();
        let tuples_from_key_zero: Vec<_> =
            map_state.iter(&db).unwrap().map(Result::unwrap).collect();
        assert_eq!(tuples_from_key_zero, expected_for_key_zero);

        map_state.clear(&mut db).unwrap();
        assert!(map_state.is_empty(&db).unwrap());

        map_state.set_current_key(1).unwrap();
        let tuples_from_key_one_after_clear_zero: Vec<_> =
            map_state.iter(&db).unwrap().map(Result::unwrap).collect();
        assert_eq!(tuples_from_key_one_after_clear_zero, expected_for_key_one);
    }
}
