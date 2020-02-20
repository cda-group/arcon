// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::ArconResult,
    state_backend::{
        in_memory::{InMemory, StateCommon},
        serialization::SerializableFixedSizeWith,
        state_types::{AppendingState, MergingState, ReducingState, State},
    },
};
use smallbox::SmallBox;
use std::marker::PhantomData;

pub struct InMemoryReducingState<IK, N, T, F, KS> {
    pub(crate) common: StateCommon<IK, N, KS>,
    pub(crate) reduce_fn: F,
    pub(crate) _phantom: PhantomData<T>,
}

impl<IK, N, T, F, KS> State<InMemory, IK, N> for InMemoryReducingState<IK, N, T, F, KS>
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

impl<IK, N, T, F, KS> AppendingState<InMemory, IK, N, T, T>
    for InMemoryReducingState<IK, N, T, F, KS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: Send + Sync + Clone + 'static,
    F: Fn(&T, &T) -> T,
{
    fn get(&self, backend: &InMemory) -> ArconResult<T> {
        let key = self.common.get_db_key_prefix()?;
        let dynamic = backend.get(&key)?;
        let value = dynamic
            .downcast_ref::<T>()
            .ok_or_else(|| arcon_err_kind!("Dynamic value has a wrong type!"))?
            .clone();

        Ok(value)
    }

    fn append(&self, backend: &mut InMemory, value: T) -> ArconResult<()> {
        let key = self.common.get_db_key_prefix()?;
        match backend.get_mut(&key) {
            // TODO: only handle value not found
            Err(_) => {
                backend.put(key, SmallBox::new(value))?;
            }
            Ok(dynamic) => {
                let old = dynamic
                    .downcast_mut::<T>()
                    .ok_or_else(|| arcon_err_kind!("Dynamic value has a wrong type!"))?;
                *old = (self.reduce_fn)(old, &value);
            }
        }

        Ok(())
    }
}

impl<IK, N, T, F, KS> MergingState<InMemory, IK, N, T, T> for InMemoryReducingState<IK, N, T, F, KS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: Send + Sync + Clone + 'static,
    F: Fn(&T, &T) -> T,
{
}

impl<IK, N, T, F, KS> ReducingState<InMemory, IK, N, T> for InMemoryReducingState<IK, N, T, F, KS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: Send + Sync + Clone + 'static,
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
