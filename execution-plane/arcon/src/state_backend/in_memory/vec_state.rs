// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::ArconResult,
    state_backend::{
        in_memory::{InMemory, StateCommon},
        serialization::SerializableFixedSizeWith,
        state_types::{AppendingState, MergingState, State, VecState},
    },
};
use smallbox::SmallBox;
use std::marker::PhantomData;

pub struct InMemoryVecState<IK, N, T, KS> {
    pub(crate) common: StateCommon<IK, N, KS>,
    pub(crate) _phantom: PhantomData<T>,
}

impl<IK, N, T, KS> State<InMemory, IK, N> for InMemoryVecState<IK, N, T, KS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
{
    fn clear(&self, backend: &mut InMemory) -> ArconResult<()> {
        let key = self.common.get_db_key_prefix()?;
        let _old_value = backend.remove(&key);
        Ok(())
    }

    delegate_key_and_namespace!(common);
}

impl<IK, N, T, KS> AppendingState<InMemory, IK, N, T, Vec<T>> for InMemoryVecState<IK, N, T, KS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: Send + Sync + Clone + 'static,
{
    fn get(&self, backend: &InMemory) -> ArconResult<Vec<T>> {
        let key = self.common.get_db_key_prefix()?;
        if let Some(dynamic) = backend.get(&key) {
            let vec = dynamic
                .downcast_ref::<Vec<T>>()
                .ok_or_else(|| arcon_err_kind!("Dynamic value has a wrong type!"))?
                .clone();

            Ok(vec)
        } else {
            Ok(vec![])
        }
    }

    fn append(&self, backend: &mut InMemory, value: T) -> ArconResult<()> {
        let key = self.common.get_db_key_prefix()?;
        let storage = backend.get_mut_or_insert(key, || SmallBox::new(Vec::<T>::new()));
        let vec = storage
            .downcast_mut::<Vec<T>>()
            .ok_or_else(|| arcon_err_kind!("Dynamic value has a wrong type!"))?;

        vec.push(value);
        Ok(())
    }
}

impl<IK, N, T, KS> MergingState<InMemory, IK, N, T, Vec<T>> for InMemoryVecState<IK, N, T, KS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: Send + Sync + Clone + 'static,
{
}

impl<IK, N, T, KS> VecState<InMemory, IK, N, T> for InMemoryVecState<IK, N, T, KS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: Send + Sync + Clone + 'static,
{
    fn set(&self, backend: &mut InMemory, value: Vec<T>) -> ArconResult<()> {
        let key = self.common.get_db_key_prefix()?;
        let _old_value = backend.insert(key, SmallBox::new(value));
        Ok(())
    }

    fn add_all(
        &self,
        backend: &mut InMemory,
        values: impl IntoIterator<Item = T>,
    ) -> ArconResult<()>
    where
        Self: Sized,
    {
        let key = self.common.get_db_key_prefix()?;
        let dynamic = backend.get_mut_or_insert(key, || SmallBox::new(Vec::<T>::new()));
        let vec = dynamic
            .downcast_mut::<Vec<T>>()
            .ok_or_else(|| arcon_err_kind!("Dynamic value has a wrong type!"))?;

        vec.extend(values);

        Ok(())
    }

    fn add_all_dyn(
        &self,
        backend: &mut InMemory,
        values: &mut dyn Iterator<Item = T>,
    ) -> ArconResult<()> {
        self.add_all(backend, values)
    }

    fn is_empty(&self, backend: &InMemory) -> ArconResult<bool> {
        let key = self.common.get_db_key_prefix()?;
        if let Some(dynamic) = backend.get(&key) {
            Ok(dynamic
                .downcast_ref::<Vec<T>>()
                .ok_or_else(|| arcon_err_kind!("Dynamic value has a wrong type!"))?
                .is_empty())
        } else {
            Ok(true)
        }
    }

    fn len(&self, backend: &InMemory) -> ArconResult<usize> {
        let key = self.common.get_db_key_prefix()?;
        if let Some(dynamic) = backend.get(&key) {
            Ok(dynamic
                .downcast_ref::<Vec<T>>()
                .ok_or_else(|| arcon_err_kind!("Dynamic value has a wrong type!"))?
                .len())
        } else {
            Ok(0)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::{
        serialization::{NativeEndianBytesDump, Prost},
        StateBackend, VecStateBuilder,
    };

    #[test]
    fn vec_state_test() {
        let mut db = InMemory::new("test").unwrap();
        let vec_state = db.new_vec_state("test_state", (), (), NativeEndianBytesDump, Prost);
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
