// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::ArconResult,
    state_backend::{
        faster::{Faster, StateCommon},
        serialization::{DeserializableWith, LittleEndianBytesDump, SerializableWith},
        state_types::{AppendingState, MergingState, State, VecState},
    },
};
use std::{marker::PhantomData, mem};

pub struct FasterVecState<IK, N, T, KS, TS> {
    pub(crate) common: StateCommon<IK, N, KS, TS>,
    pub(crate) _phantom: PhantomData<T>,
}

impl<IK, N, T, KS, TS> State<Faster, IK, N> for FasterVecState<IK, N, T, KS, TS>
where
    IK: SerializableWith<KS>,
    N: SerializableWith<KS>,
{
    fn clear(&self, backend: &mut Faster) -> ArconResult<()> {
        let key = self.common.get_db_key_prefix()?;
        backend.remove(&key)?;
        Ok(())
    }

    delegate_key_and_namespace!(common);
}

impl<IK, N, T, KS, TS> AppendingState<Faster, IK, N, T, Vec<T>> for FasterVecState<IK, N, T, KS, TS>
where
    IK: SerializableWith<KS>,
    N: SerializableWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
{
    fn get(&self, backend: &Faster) -> ArconResult<Vec<T>> {
        backend.in_session(|backend| {
            let key = self.common.get_db_key_prefix()?;
            if let Some(serialized) = backend.get_vec(&key)? {
                let res = serialized
                    .into_iter()
                    .map(|s| T::deserialize(&self.common.value_serializer, &s))
                    .collect::<ArconResult<Vec<_>>>()?;

                Ok(res)
            } else {
                Ok(vec![])
            }
        })
    }

    fn append(&self, backend: &mut Faster, value: T) -> ArconResult<()> {
        backend.in_session_mut(|backend| {
            let key = self.common.get_db_key_prefix()?;
            let mut serialized = T::serialize(&self.common.value_serializer, &value)?;

            backend.vec_push(&key, serialized)
        })
    }
}

impl<IK, N, T, KS, TS> MergingState<Faster, IK, N, T, Vec<T>> for FasterVecState<IK, N, T, KS, TS>
where
    IK: SerializableWith<KS>,
    N: SerializableWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
{
}

impl<IK, N, T, KS, TS> VecState<Faster, IK, N, T> for FasterVecState<IK, N, T, KS, TS>
where
    IK: SerializableWith<KS>,
    N: SerializableWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
{
    fn set(&self, backend: &mut Faster, value: Vec<T>) -> ArconResult<()> {
        backend.in_session_mut(|backend| {
            let key = self.common.get_db_key_prefix()?;
            let storage = value
                .into_iter()
                .map(|v| T::serialize(&self.common.value_serializer, &v))
                .collect::<ArconResult<Vec<_>>>()?;

            backend.vec_set(&key, &storage)
        })
    }

    fn add_all(&self, backend: &mut Faster, values: impl IntoIterator<Item = T>) -> ArconResult<()>
    where
        Self: Sized,
    {
        backend.in_session_mut(|backend| {
            let key = self.common.get_db_key_prefix()?;

            for value in values {
                let serialized = T::serialize(&self.common.value_serializer, &value)?;
                backend.vec_push(&key, serialized)?;
            }
            Ok(())
        })
    }

    fn add_all_dyn(
        &self,
        backend: &mut Faster,
        values: &mut dyn Iterator<Item = T>,
    ) -> ArconResult<()> {
        self.add_all(backend, values)
    }

    fn is_empty(&self, backend: &Faster) -> ArconResult<bool> {
        backend.in_session(|backend| {
            let key = self.common.get_db_key_prefix()?;
            Ok(backend.get_vec(&key)?.map(|v| v.is_empty()).unwrap_or(true))
        })
    }

    fn len(&self, backend: &Faster) -> ArconResult<usize> {
        backend.in_session(|backend| {
            let key = self.common.get_db_key_prefix()?;
            Ok(backend.get_vec(&key)?.map(|v| v.len()).unwrap_or(0))
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::{
        faster::test::TestDb, serialization::NativeEndianBytesDump, VecStateBuilder,
    };

    #[test]
    fn vec_state_test() {
        let mut db = TestDb::new();
        let vec_state = db.new_vec_state(
            "test_state",
            (),
            (),
            NativeEndianBytesDump,
            NativeEndianBytesDump,
        );
        assert!(vec_state.is_empty(&db).unwrap());
        assert_eq!(vec_state.len(&db).unwrap(), 0);

        vec_state.append(&mut db, 1).unwrap();
        vec_state.append(&mut db, 2).unwrap();
        vec_state.append(&mut db, 3).unwrap();
        vec_state.add_all(&mut db, vec![4, 5, 6]).unwrap();

        assert_eq!(vec_state.get(&db).unwrap(), vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(vec_state.len(&db).unwrap(), 6);
    }
}
