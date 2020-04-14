// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::ArconResult,
    state_backend::{
        faster::{Faster, StateCommon},
        serialization::{DeserializableWith, SerializableWith},
        state_types::{AppendingState, MergingState, ReducingState, State},
    },
};
use std::marker::PhantomData;

pub struct FasterReducingState<IK, N, T, F, KS, TS> {
    pub(crate) common: StateCommon<IK, N, KS, TS>,
    pub(crate) reduce_fn: Box<dyn Fn(&[u8], &[u8]) -> Vec<u8> + Send + Sync>,
    pub(crate) _phantom: PhantomData<(T, F)>,
}

impl<IK, N, T, F, KS, TS> State<Faster, IK, N> for FasterReducingState<IK, N, T, F, KS, TS>
where
    IK: SerializableWith<KS>,
    N: SerializableWith<KS>,
{
    fn clear(&self, backend: &mut Faster) -> ArconResult<()> {
        backend.in_session_mut(|backend| {
            let key = self.common.get_db_key_prefix()?;
            backend.remove(&key)?;
            Ok(())
        })
    }

    delegate_key_and_namespace!(common);
}

pub fn make_reduce_fn<T, F, TS>(
    fun: F,
    serializer: TS,
) -> Box<dyn Fn(&[u8], &[u8]) -> Vec<u8> + Send + Sync>
where
    T: SerializableWith<TS> + DeserializableWith<TS>,
    F: Fn(&T, &T) -> T + Send + Sync + 'static,
    TS: Send + Sync + 'static,
{
    Box::new(move |old, new| {
        let old = T::deserialize(&serializer, old).expect("Could not deserialize old value");
        let new = T::deserialize(&serializer, new).expect("Could not deserialize new value");

        let res = fun(&old, &new);

        T::serialize(&serializer, &res).expect("Could not serialize the result")
    })
}

impl<IK, N, T, F, KS, TS> AppendingState<Faster, IK, N, T, Option<T>>
    for FasterReducingState<IK, N, T, F, KS, TS>
where
    IK: SerializableWith<KS>,
    N: SerializableWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
    F: Fn(&T, &T) -> T,
{
    fn get(&self, backend: &Faster) -> ArconResult<Option<T>> {
        backend.in_session(|backend| {
            let key = self.common.get_db_key_prefix()?;
            if let Some(storage) = backend.get_agg(&key)? {
                let value = T::deserialize(&self.common.value_serializer, &*storage)?;
                Ok(Some(value))
            } else {
                Ok(None)
            }
        })
    }

    fn append(&self, backend: &mut Faster, value: T) -> ArconResult<()> {
        let key = self.common.get_db_key_prefix()?;
        let serialized = T::serialize(&self.common.value_serializer, &value)?;

        backend.in_session_mut(|backend| backend.aggregate(&key, serialized, &*self.reduce_fn))
    }
}

impl<IK, N, T, F, KS, TS> MergingState<Faster, IK, N, T, Option<T>>
    for FasterReducingState<IK, N, T, F, KS, TS>
where
    IK: SerializableWith<KS>,
    N: SerializableWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
    F: Fn(&T, &T) -> T,
{
}

impl<IK, N, T, F, KS, TS> ReducingState<Faster, IK, N, T>
    for FasterReducingState<IK, N, T, F, KS, TS>
where
    IK: SerializableWith<KS>,
    N: SerializableWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
    F: Fn(&T, &T) -> T,
{
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::{
        faster::test::TestDb, serialization::NativeEndianBytesDump, ReducingStateBuilder,
    };

    #[test]
    fn reducing_state_test() {
        let mut db = TestDb::new();
        let reducing_state = db.new_reducing_state(
            "test_state",
            (),
            (),
            |old: &i32, new: &i32| *old.max(new),
            NativeEndianBytesDump,
            NativeEndianBytesDump,
        );

        reducing_state.append(&mut db, 7).unwrap();
        reducing_state.append(&mut db, 42).unwrap();
        reducing_state.append(&mut db, 10).unwrap();

        assert_eq!(reducing_state.get(&db).unwrap().unwrap(), 42);
    }

    #[test]
    fn different_funcs_test() {
        let mut db = TestDb::new();
        let rs1 = db.new_reducing_state(
            "rs1",
            (),
            (),
            |old: &i32, new: &i32| *old.max(new),
            NativeEndianBytesDump,
            NativeEndianBytesDump,
        );
        let rs2 = db.new_reducing_state(
            "rs2",
            (),
            (),
            |old: &i32, new: &i32| *old.min(new),
            NativeEndianBytesDump,
            NativeEndianBytesDump,
        );

        rs1.append(&mut db, 7).unwrap();
        rs2.append(&mut db, 7).unwrap();
        rs1.append(&mut db, 42).unwrap();
        rs2.append(&mut db, 42).unwrap();
        rs1.append(&mut db, 10).unwrap();
        rs2.append(&mut db, 10).unwrap();

        assert_eq!(rs1.get(&db).unwrap().unwrap(), 42);
        assert_eq!(rs2.get(&db).unwrap().unwrap(), 7);
    }
}
