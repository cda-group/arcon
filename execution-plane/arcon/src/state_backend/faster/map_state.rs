// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::ArconResult,
    state_backend::{
        faster::{Faster, StateCommon},
        serialization::{
            DeserializableWith, LittleEndianBytesDump, SerializableFixedSizeWith, SerializableWith,
        },
        state_types::{BoxedIteratorOfArconResult, MapState, State},
    },
};
use std::marker::PhantomData;

pub struct FasterMapState<IK, N, K, V, KS, TS> {
    pub(crate) common: StateCommon<IK, N, KS, TS>,
    pub(crate) _phantom: PhantomData<(K, V)>,
}

// we keep each key-value pair as its own entry in faster and then also an additional entry with
// all the keys in the map

impl<IK, N, K, V, KS, TS> State<Faster, IK, N> for FasterMapState<IK, N, K, V, KS, TS>
where
    IK: SerializableWith<KS>,
    N: SerializableWith<KS>,
{
    fn clear(&self, backend: &mut Faster) -> ArconResult<()> {
        backend.in_session_mut(|backend| {
            let prefix = self.common.get_db_key_prefix()?;
            let keys = backend.get_vec(&prefix)?;
            let keys = if let Some(k) = keys { k } else { return Ok(()) };
            backend.remove(&prefix)?;
            for key in keys {
                backend.remove(&key)?;
            }
            Ok(())
        })
    }

    delegate_key_and_namespace!(common);
}

impl<IK, N, K, V, KS, TS> MapState<Faster, IK, N, K, V> for FasterMapState<IK, N, K, V, KS, TS>
where
    IK: SerializableWith<KS> + DeserializableWith<KS>,
    N: SerializableWith<KS> + DeserializableWith<KS>,
    K: SerializableWith<KS> + DeserializableWith<KS>,
    V: SerializableWith<TS> + DeserializableWith<TS>,
    KS: Clone + 'static,
    TS: Clone + 'static,
{
    fn get(&self, backend: &Faster, key: &K) -> ArconResult<Option<V>> {
        backend.in_session(|backend| {
            let key = self.common.get_db_key_with_user_key(key)?;
            if let Some(serialized) = backend.get(&key)? {
                let value = V::deserialize(&self.common.value_serializer, &serialized)?;
                Ok(Some(value))
            } else {
                Ok(None)
            }
        })
    }

    fn fast_insert(&self, backend: &mut Faster, key: K, value: V) -> ArconResult<()> {
        backend.in_session_mut(|backend| {
            let prefix = self.common.get_db_key_prefix()?;
            let key = self.common.get_db_key_with_user_key(&key)?;
            let serialized = V::serialize(&self.common.value_serializer, &value)?;
            backend.put(&key, &serialized)?;
            backend.vec_push_if_absent(&prefix, key)?;

            Ok(())
        })
    }

    fn insert(&self, backend: &mut Faster, key: K, value: V) -> ArconResult<Option<V>> {
        backend.in_session_mut(|backend| {
            let prefix = self.common.get_db_key_prefix()?;
            let key = self.common.get_db_key_with_user_key(&key)?;

            let old = if let Some(slice) = backend.get(&key)? {
                Some(V::deserialize(&self.common.value_serializer, &*slice)?)
            } else {
                None
            };

            let serialized = V::serialize(&self.common.value_serializer, &value)?;
            backend.put(&key, &serialized)?;
            backend.vec_push_if_absent(&prefix, key)?;

            Ok(old)
        })
    }

    fn insert_all_dyn(
        &self,
        backend: &mut Faster,
        key_value_pairs: &mut dyn Iterator<Item = (K, V)>,
    ) -> ArconResult<()> {
        self.insert_all(backend, key_value_pairs)
    }

    fn insert_all(
        &self,
        backend: &mut Faster,
        key_value_pairs: impl IntoIterator<Item = (K, V)>,
    ) -> ArconResult<()>
    where
        Self: Sized,
    {
        backend.in_session_mut(|backend| {
            let prefix = self.common.get_db_key_prefix()?;
            for (user_key, value) in key_value_pairs {
                let key = self.common.get_db_key_with_user_key(&user_key)?;
                let serialized = V::serialize(&self.common.value_serializer, &value)?;
                backend.put(&key, &serialized)?;
                backend.vec_push_if_absent(&prefix, key)?;
            }

            Ok(())
        })
    }

    fn remove(&self, backend: &mut Faster, key: &K) -> ArconResult<()> {
        backend.in_session_mut(|backend| {
            let prefix = self.common.get_db_key_prefix()?;
            let key = self.common.get_db_key_with_user_key(key)?;
            backend.remove(&key)?;
            backend.vec_remove(&prefix, key)?;

            Ok(())
        })
    }

    fn contains(&self, backend: &Faster, key: &K) -> ArconResult<bool> {
        backend.in_session(|backend| {
            let prefix = self.common.get_db_key_prefix()?;
            let key = self.common.get_db_key_with_user_key(key)?;
            let keys = backend.get_vec(&prefix)?;
            if let Some(keys) = keys {
                Ok(keys.contains(&key))
            } else {
                Ok(false)
            }
        })
    }

    // TODO: unboxed versions of below
    fn iter<'a>(&self, backend: &'a Faster) -> ArconResult<BoxedIteratorOfArconResult<'a, (K, V)>> {
        use std::iter;
        let prefix = self.common.get_db_key_prefix()?;
        let keys = backend.in_session(|backend| backend.get_vec(&prefix))?;

        let key_serializer = self.common.key_serializer.clone();
        let value_serializer = self.common.value_serializer.clone();

        let iter = keys.into_iter().flatten().map(move |serialized_key| {
            // TODO: multiple sessions opened and closed often
            backend.in_session(|backend| {
                let serialized_value = backend
                    .get(&serialized_key)?
                    .ok_or(arcon_err_kind!("Value not found"))?;

                let mut key_cursor = &serialized_key[..];
                let _ = Vec::<u8>::deserialize_from(&LittleEndianBytesDump, &mut key_cursor)?;
                let _ = IK::deserialize_from(&key_serializer, &mut key_cursor)?;
                let _ = N::deserialize_from(&key_serializer, &mut key_cursor)?;
                let key = K::deserialize_from(&key_serializer, &mut key_cursor)?;
                let value = V::deserialize(&value_serializer, &serialized_value)?;
                Ok((key, value))
            })
        });

        Ok(Box::new(iter))
    }

    fn keys<'a>(&self, backend: &'a Faster) -> ArconResult<BoxedIteratorOfArconResult<'a, K>> {
        use std::iter;
        let prefix = self.common.get_db_key_prefix()?;
        let keys = backend.in_session(|backend| backend.get_vec(&prefix))?;

        let key_serializer = self.common.key_serializer.clone();

        let iter = keys.into_iter().flatten().map(move |serialized_key| {
            // TODO: multiple sessions opened and closed often
            let mut key_cursor = &serialized_key[..];
            let _ = Vec::<u8>::deserialize_from(&LittleEndianBytesDump, &mut key_cursor)?;
            let _ = IK::deserialize_from(&key_serializer, &mut key_cursor)?;
            let _ = N::deserialize_from(&key_serializer, &mut key_cursor)?;
            let key = K::deserialize_from(&key_serializer, &mut key_cursor)?;
            Ok(key)
        });

        Ok(Box::new(iter))
    }

    fn values<'a>(&self, backend: &'a Faster) -> ArconResult<BoxedIteratorOfArconResult<'a, V>> {
        use std::iter;
        let prefix = self.common.get_db_key_prefix()?;
        let keys = backend.in_session(|backend| backend.get_vec(&prefix))?;

        let value_serializer = self.common.value_serializer.clone();

        let iter = keys.into_iter().flatten().map(move |serialized_key| {
            // TODO: multiple sessions opened and closed often
            backend.in_session(|backend| {
                let serialized_value = backend
                    .get(&serialized_key)?
                    .ok_or(arcon_err_kind!("Value not found"))?;

                let value = V::deserialize(&value_serializer, &serialized_value)?;
                Ok(value)
            })
        });

        Ok(Box::new(iter))
    }

    fn len(&self, backend: &Faster) -> ArconResult<usize> {
        backend.in_session(|backend| {
            let prefix = self.common.get_db_key_prefix()?;
            let keys = backend.get_vec(&prefix)?;
            Ok(keys.map(|keys| keys.len()).unwrap_or(0))
        })
    }

    fn is_empty(&self, backend: &Faster) -> ArconResult<bool> {
        backend.in_session(|backend| {
            let prefix = self.common.get_db_key_prefix()?;
            let keys = backend.get_vec(&prefix)?;
            Ok(keys.map(|keys| keys.is_empty()).unwrap_or(true))
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::{
        faster::test::TestDb, serialization::NativeEndianBytesDump, MapStateBuilder,
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
