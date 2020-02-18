// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::ArconResult,
    state_backend::{
        in_memory::{InMemory, StateCommon},
        serialization::{DeserializableWith, SerializableFixedSizeWith, SerializableWith},
        state_types::{AppendingState, MergingState, State, VecState},
    },
};
//use error::ErrorKind;
use std::marker::PhantomData;

pub struct InMemoryVecState<IK, N, T, KS, TS> {
    pub(crate) common: StateCommon<IK, N, KS, TS>,
    pub(crate) _phantom: PhantomData<T>,
}

impl<IK, N, T, KS, TS> State<InMemory, IK, N> for InMemoryVecState<IK, N, T, KS, TS>
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

impl<IK, N, T, KS, TS> AppendingState<InMemory, IK, N, T, Vec<T>>
    for InMemoryVecState<IK, N, T, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
{
    fn get(&self, backend: &InMemory) -> ArconResult<Vec<T>> {
        let key = self.common.get_db_key_prefix()?;
        let serialized = backend.get(&key)?;

        // cursor is updated in the loop to point at the yet unconsumed part of the serialized data
        let mut cursor = &serialized[..];
        let mut res = vec![];
        while !cursor.is_empty() {
            let val = T::deserialize_from(&self.common.value_serializer, &mut cursor)?;
            res.push(val);
        }

        Ok(res)
    }

    fn append(&self, backend: &mut InMemory, value: T) -> ArconResult<()> {
        let key = self.common.get_db_key_prefix()?;
        let storage = backend.get_mut_or_init_empty(&key)?;

        T::serialize_into(&self.common.value_serializer, storage, &value)
    }
}

impl<IK, N, T, KS, TS> MergingState<InMemory, IK, N, T, Vec<T>>
    for InMemoryVecState<IK, N, T, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
{
}

impl<IK, N, T, KS, TS> VecState<InMemory, IK, N, T> for InMemoryVecState<IK, N, T, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
{
    fn set(&self, backend: &mut InMemory, value: Vec<T>) -> ArconResult<()> {
        let key = self.common.get_db_key_prefix()?;
        let mut storage = vec![];
        for elem in value {
            T::serialize_into(&self.common.value_serializer, &mut storage, &elem)?;
        }
        backend.put(key, storage)
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
        let mut storage = backend.get_mut_or_init_empty(&key)?;

        for value in values {
            T::serialize_into(&self.common.value_serializer, &mut storage, &value)?;
        }

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
        if let Ok(storage) = backend.get(&key) {
            Ok(storage.is_empty())
        } else {
            Ok(true)
        }
    }

    //
    //    fn len(&self, backend: &InMemory) -> ArconResult<usize>
    //    where
    //        T: SerializableFixedSizeWith<TS>,
    //    {
    //        let key = self.common.get_db_key(&())?;
    //        let storage = backend.get(&key);
    //
    //        match storage {
    //            Err(e) => match e.kind() {
    //                ErrorKind::ArconError(message) if &*message == "Value not found" => Ok(0),
    //                _ => Err(e),
    //            },
    //            Ok(buf) => {
    //                if buf.is_empty() {
    //                    return Ok(0);
    //                }
    //
    //                debug_assert_ne!(T::SIZE, 0);
    //
    //                let len = buf.len() / T::SIZE;
    //                let rem = buf.len() % T::SIZE;
    //
    //                // sanity check
    //                if rem != 0 {
    //                    return arcon_err!(
    //                        "vec state storage length is not a multiple of element size"
    //                    );
    //                }
    //
    //                Ok(len)
    //            }
    //        }
    //    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::{serialization::Bincode, StateBackend, VecStateBuilder};

    #[test]
    fn vec_state_test() {
        let mut db = InMemory::new("test").unwrap();
        let vec_state = db.new_vec_state("test_state", (), (), Bincode, Bincode);
        assert!(vec_state.is_empty(&db).unwrap());
        //        assert_eq!(vec_state.len(&db).unwrap(), 0);

        vec_state.append(&mut db, 1).unwrap();
        vec_state.append(&mut db, 2).unwrap();
        vec_state.append(&mut db, 3).unwrap();
        vec_state.add_all(&mut db, vec![4, 5, 6]).unwrap();

        assert_eq!(vec_state.get(&db).unwrap(), vec![1, 2, 3, 4, 5, 6]);
        //        assert_eq!(vec_state.len(&db).unwrap(), 6);
    }
}
