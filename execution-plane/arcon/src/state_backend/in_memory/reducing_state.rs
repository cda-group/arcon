// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::ArconResult,
    state_backend::{
        in_memory::{InMemory, StateCommon},
        serialization::{DeserializableWith, SerializableFixedSizeWith, SerializableWith},
        state_types::{AppendingState, MergingState, ReducingState, State},
    },
};
use std::marker::PhantomData;

pub struct InMemoryReducingState<IK, N, T, F, KS, TS> {
    pub(crate) common: StateCommon<IK, N, KS, TS>,
    pub(crate) reduce_fn: F,
    pub(crate) _phantom: PhantomData<T>,
}

impl<IK, N, T, F, KS, TS> State<InMemory, IK, N> for InMemoryReducingState<IK, N, T, F, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
{
    fn clear(&self, backend: &mut InMemory) -> ArconResult<()> {
        let key = self.common.get_db_key_prefix()?;
        backend.remove(&key)?;
        Ok(())
    }

    delegate_key_and_namespace!(common);
}

impl<IK, N, T, F, KS, TS> AppendingState<InMemory, IK, N, T, T>
    for InMemoryReducingState<IK, N, T, F, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
    F: Fn(&T, &T) -> T,
{
    fn get(&self, backend: &InMemory) -> ArconResult<T> {
        let key = self.common.get_db_key_prefix()?;
        let storage = backend.get(&key)?;
        let value = T::deserialize(&self.common.value_serializer, storage)?;

        Ok(value)
    }

    fn append(&self, backend: &mut InMemory, value: T) -> ArconResult<()> {
        let key = self.common.get_db_key_prefix()?;
        let storage = backend.get_mut_or_init_empty(&key)?;
        if storage.is_empty() {
            T::serialize_into(&self.common.value_serializer, storage, &value)?;
            return Ok(());
        }

        let old_value = T::deserialize(&self.common.value_serializer, storage)?;

        let new_value = (self.reduce_fn)(&old_value, &value);
        T::serialize_into(
            &self.common.value_serializer,
            storage.as_mut_slice(),
            &new_value,
        )?;

        Ok(())
    }
}

impl<IK, N, T, F, KS, TS> MergingState<InMemory, IK, N, T, T>
    for InMemoryReducingState<IK, N, T, F, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
    F: Fn(&T, &T) -> T,
{
}

impl<IK, N, T, F, KS, TS> ReducingState<InMemory, IK, N, T>
    for InMemoryReducingState<IK, N, T, F, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
    F: Fn(&T, &T) -> T,
{
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::{serialization::Bincode, ReducingStateBuilder, StateBackend};

    #[test]
    fn reducing_state_test() {
        let mut db = InMemory::new("test").unwrap();
        let reducing_state = db.new_reducing_state(
            "test_state",
            (),
            (),
            |old: &i32, new: &i32| *old.max(new),
            Bincode,
            Bincode,
        );

        reducing_state.append(&mut db, 7).unwrap();
        reducing_state.append(&mut db, 42).unwrap();
        reducing_state.append(&mut db, 10).unwrap();

        assert_eq!(reducing_state.get(&db).unwrap(), 42);
    }
}
