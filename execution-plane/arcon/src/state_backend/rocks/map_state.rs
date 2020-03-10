// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::ArconResult,
    state_backend::{
        rocks::{state_common::StateCommon, RocksDb},
        serialization::{DeserializableWith, SerializableFixedSizeWith, SerializableWith},
        state_types::{BoxedIteratorOfArconResult, MapState, State},
    },
};
use rocksdb::WriteBatch;
use std::marker::PhantomData;

pub struct RocksDbMapState<IK, N, K, V, KS, TS> {
    pub(crate) common: StateCommon<IK, N, KS, TS>,
    pub(crate) _phantom: PhantomData<(K, V)>,
}

impl<IK, N, K, V, KS, TS> State<RocksDb, IK, N> for RocksDbMapState<IK, N, K, V, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
{
    fn clear(&self, backend: &mut RocksDb) -> ArconResult<()> {
        let prefix = self.common.get_db_key_prefix()?;
        backend.remove_prefix(&self.common.cf_name, prefix)
    }

    delegate_key_and_namespace!(common);
}

impl<IK, N, K, V, KS, TS> MapState<RocksDb, IK, N, K, V> for RocksDbMapState<IK, N, K, V, KS, TS>
where
    IK: SerializableFixedSizeWith<KS> + DeserializableWith<KS>,
    N: SerializableFixedSizeWith<KS> + DeserializableWith<KS>,
    K: SerializableWith<KS> + DeserializableWith<KS>,
    V: SerializableWith<TS> + DeserializableWith<TS>,
    KS: Clone + 'static,
    TS: Clone + 'static,
{
    fn get(&self, backend: &RocksDb, key: &K) -> ArconResult<Option<V>> {
        let key = self.common.get_db_key_with_user_key(key)?;
        if let Some(serialized) = backend.get(&self.common.cf_name, &key)? {
            let value = V::deserialize(&self.common.value_serializer, &serialized)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn fast_insert(&self, backend: &mut RocksDb, key: K, value: V) -> ArconResult<()> {
        let key = self.common.get_db_key_with_user_key(&key)?;
        let serialized = V::serialize(&self.common.value_serializer, &value)?;
        backend.put(&self.common.cf_name, key, serialized)?;

        Ok(())
    }

    // NOTE: not atomic!
    fn insert(&self, backend: &mut RocksDb, key: K, value: V) -> ArconResult<Option<V>> {
        let key = self.common.get_db_key_with_user_key(&key)?;

        // couldn't find a `put` that would return the previous value from rocks
        let old = if let Some(slice) = backend.get(&self.common.cf_name, &key)? {
            Some(V::deserialize(&self.common.value_serializer, &*slice)?)
        } else {
            None
        };

        let serialized = V::serialize(&self.common.value_serializer, &value)?;
        backend.put(&self.common.cf_name, key, serialized)?;

        Ok(old)
    }

    fn insert_all_dyn(
        &self,
        backend: &mut RocksDb,
        key_value_pairs: &mut dyn Iterator<Item = (K, V)>,
    ) -> ArconResult<()> {
        self.insert_all(backend, key_value_pairs)
    }

    fn insert_all(
        &self,
        backend: &mut RocksDb,
        key_value_pairs: impl IntoIterator<Item = (K, V)>,
    ) -> ArconResult<()>
    where
        Self: Sized,
    {
        let backend = backend.initialized_mut()?;

        let mut wb = WriteBatch::default();
        let cf = backend.get_cf_handle(&self.common.cf_name)?;

        for (user_key, value) in key_value_pairs {
            let key = self.common.get_db_key_with_user_key(&user_key)?;
            let serialized = V::serialize(&self.common.value_serializer, &value)?;
            wb.put_cf(cf, key, serialized)
                .map_err(|e| arcon_err_kind!("Could not create put operation: {}", e))?;
        }

        backend
            .db
            .write(wb)
            .map_err(|e| arcon_err_kind!("Could not perform batch put operation: {}", e))
    }

    fn remove(&self, backend: &mut RocksDb, key: &K) -> ArconResult<()> {
        let key = self.common.get_db_key_with_user_key(key)?;
        backend.remove(&self.common.cf_name, &key)?;

        Ok(())
    }

    fn contains(&self, backend: &RocksDb, key: &K) -> ArconResult<bool> {
        let key = self.common.get_db_key_with_user_key(key)?;
        backend.contains(&self.common.cf_name, &key)
    }

    // TODO: unboxed versions of below
    fn iter<'a>(
        &self,
        backend: &'a RocksDb,
    ) -> ArconResult<BoxedIteratorOfArconResult<'a, (K, V)>> {
        let backend = backend.initialized()?;

        let prefix = self.common.get_db_key_prefix()?;
        let cf = backend.get_cf_handle(&self.common.cf_name)?;
        let key_serializer = self.common.key_serializer.clone();
        let value_serializer = self.common.value_serializer.clone();
        // NOTE: prefix_iterator only works as expected when the cf has proper prefix_extractor
        //   option set. We do that in RocksDb::create_cf_options_for
        let iter = backend
            .db
            .prefix_iterator_cf(cf, prefix)
            .map_err(|e| arcon_err_kind!("Could not create prefix iterator: {}", e))?
            .map(move |(db_key, serialized_value)| {
                let mut key_cursor = &db_key[..];
                let _item_key = IK::deserialize_from(&key_serializer, &mut key_cursor)?;
                let _namespace = N::deserialize_from(&key_serializer, &mut key_cursor)?;
                let key = K::deserialize_from(&key_serializer, &mut key_cursor)?;
                let value = V::deserialize(&value_serializer, &serialized_value)?;

                Ok((key, value))
            });

        Ok(Box::new(iter))
    }

    fn keys<'a>(&self, backend: &'a RocksDb) -> ArconResult<BoxedIteratorOfArconResult<'a, K>> {
        let backend = backend.initialized()?;

        let prefix = self.common.get_db_key_prefix()?;
        let cf = backend.get_cf_handle(&self.common.cf_name)?;
        let key_serializer = self.common.key_serializer.clone();

        let iter = backend
            .db
            .prefix_iterator_cf(cf, prefix)
            .map_err(|e| arcon_err_kind!("Could not create prefix iterator: {}", e))?
            .map(move |(db_key, _)| {
                let mut key_cursor = &db_key[..];
                let _ = IK::deserialize_from(&key_serializer, &mut key_cursor)?;
                let _ = N::deserialize_from(&key_serializer, &mut key_cursor)?;
                let key = K::deserialize_from(&key_serializer, &mut key_cursor)?;

                Ok(key)
            });

        Ok(Box::new(iter))
    }

    fn values<'a>(&self, backend: &'a RocksDb) -> ArconResult<BoxedIteratorOfArconResult<'a, V>> {
        let backend = backend.initialized()?;

        let prefix = self.common.get_db_key_prefix()?;
        let cf = backend.get_cf_handle(&self.common.cf_name)?;
        let value_serializer = self.common.value_serializer.clone();

        let iter = backend
            .db
            .prefix_iterator_cf(cf, prefix)
            .map_err(|e| arcon_err_kind!("Could not create prefix iterator: {}", e))?
            .map(move |(_, serialized_value)| {
                let value: V = V::deserialize(&value_serializer, &serialized_value)?;

                Ok(value)
            });

        Ok(Box::new(iter))
    }

    fn len(&self, backend: &RocksDb) -> ArconResult<usize> {
        let backend = backend.initialized()?;

        let prefix = self.common.get_db_key_prefix()?;
        let cf = backend.get_cf_handle(&self.common.cf_name)?;

        let count = backend
            .db
            .prefix_iterator_cf(cf, prefix)
            .map_err(|e| arcon_err_kind!("Could not create prefix iterator: {}", e))?
            .count();

        Ok(count)
    }

    fn is_empty(&self, backend: &RocksDb) -> ArconResult<bool> {
        let backend = backend.initialized()?;

        let prefix = self.common.get_db_key_prefix()?;
        let cf = backend.get_cf_handle(&self.common.cf_name)?;
        Ok(backend
            .db
            .prefix_iterator_cf(cf, prefix)
            .map_err(|e| arcon_err_kind!("Could not create prefix iterator: {}", e))?
            .next()
            .is_none())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::{
        rocks::test::TestDb, serialization::NativeEndianBytesDump, MapStateBuilder,
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
