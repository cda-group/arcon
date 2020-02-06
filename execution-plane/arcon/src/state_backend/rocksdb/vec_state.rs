// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use super::rocksdb::MergeOperands;
use crate::{
    prelude::ArconResult,
    state_backend::{
        rocksdb::{state_common::StateCommon, RocksDb},
        serialization::{DeserializableWith, SerializableFixedSizeWith, SerializableWith},
        state_types::{AppendingState, MergingState, State, VecState},
    },
};
use rocksdb::WriteBatch;
use std::marker::PhantomData;

pub struct RocksDbVecState<IK, N, T, KS, TS> {
    pub(crate) common: StateCommon<IK, N, KS, TS>,
    pub(crate) _phantom: PhantomData<T>,
}

impl<IK, N, T, KS, TS> State<RocksDb, IK, N, KS, TS> for RocksDbVecState<IK, N, T, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    (): SerializableWith<KS>,
{
    fn clear(&self, backend: &mut RocksDb) -> ArconResult<()> {
        let key = self.common.get_db_key(&())?;
        backend.remove(&self.common.cf_name, &key)?;
        Ok(())
    }

    delegate_key_and_namespace!(common);
}

pub fn vec_merge(_key: &[u8], first: Option<&[u8]>, rest: &mut MergeOperands) -> Option<Vec<u8>> {
    let mut result: Vec<u8> = Vec::with_capacity(rest.size_hint().0);
    first.map(|v| {
        result.extend_from_slice(v);
    });
    for op in rest {
        result.extend_from_slice(op);
    }
    Some(result)
}

impl<IK, N, T, KS, TS> AppendingState<RocksDb, IK, N, T, Vec<T>, KS, TS>
    for RocksDbVecState<IK, N, T, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    (): SerializableWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
{
    fn get(&self, backend: &RocksDb) -> ArconResult<Vec<T>> {
        let key = self.common.get_db_key(&())?;
        let serialized = backend.get(&self.common.cf_name, &key)?;

        // reader is updated in the loop to point at the yet unconsumed part of the serialized data
        let mut reader = &serialized[..];
        let mut res = vec![];
        while !reader.is_empty() {
            let val = T::deserialize_from(&self.common.value_serializer, &mut reader)?;
            res.push(val);
        }

        Ok(res)
    }

    fn append(&self, backend: &mut RocksDb, value: T) -> ArconResult<()> {
        let backend = backend.initialized_mut()?;

        let key = self.common.get_db_key(&())?;
        let serialized = T::serialize(&self.common.value_serializer, &value)?;

        let cf = backend.get_cf_handle(&self.common.cf_name)?;
        // See the vec_merge function in this module. It is set as the merge operator for every vec state.
        backend
            .db
            .merge_cf(cf, key, serialized)
            .map_err(|e| arcon_err_kind!("Could not perform merge operation: {}", e))
    }
}

impl<IK, N, T, KS, TS> MergingState<RocksDb, IK, N, T, Vec<T>, KS, TS>
    for RocksDbVecState<IK, N, T, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    (): SerializableWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
{
}

impl<IK, N, T, KS, TS> VecState<RocksDb, IK, N, T, KS, TS> for RocksDbVecState<IK, N, T, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    (): SerializableWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
{
    fn set(&self, backend: &mut RocksDb, value: Vec<T>) -> ArconResult<()> {
        let key = self.common.get_db_key(&())?;
        let mut storage = vec![];
        for elem in value {
            T::serialize_into(&self.common.value_serializer, &mut storage, &elem)?;
        }
        backend.put(&self.common.cf_name, key, storage)
    }

    fn add_all(&self, backend: &mut RocksDb, values: impl IntoIterator<Item = T>) -> ArconResult<()>
    where
        Self: Sized,
    {
        let backend = backend.initialized_mut()?;

        let key = self.common.get_db_key(&())?;
        let mut wb = WriteBatch::default();
        let cf = backend.get_cf_handle(&self.common.cf_name)?;

        for value in values {
            let serialized = T::serialize(&self.common.value_serializer, &value)?;

            wb.merge_cf(cf, &key, serialized)
                .map_err(|e| arcon_err_kind!("Could not create merge operation: {}", e))?;
        }

        backend
            .db
            .write(wb)
            .map_err(|e| arcon_err_kind!("Could not execute merge operation: {}", e))?;

        Ok(())
    }

    fn add_all_dyn(
        &self,
        backend: &mut RocksDb,
        values: &mut dyn Iterator<Item = T>,
    ) -> ArconResult<()> {
        self.add_all(backend, values)
    }

    //    /// for types that don't satisfy the extra bounds, do .get().len()
    //    fn len(&self, backend: &RocksDb) -> ArconResult<usize>
    //    where
    //        T: SerializableFixedSizeWith<TS>,
    //    {
    //        let key = self.common.get_db_key(&())?;
    //        let storage = backend.get(&self.common.cf_name, &key);
    //
    //        match storage {
    //            Err(e) => match e.kind() {
    //                ErrorKind::ArconError(message) if &*message == "Value not found" => Ok(0),
    //                _ => Err(e),
    //            },
    //            Ok(buf) => {
    //                let mut reader = &*buf;
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
    use crate::state_backend::{rocksdb::tests::TestDb, serialization::Bincode, VecStateBuilder};

    #[test]
    fn vec_state_test() {
        let mut db = TestDb::new();
        let vec_state = db.new_vec_state("test_state", (), (), Bincode, Bincode);
        //        assert_eq!(vec_state.len(&db).unwrap(), 0);

        vec_state.append(&mut db, 1).unwrap();
        vec_state.append(&mut db, 2).unwrap();
        vec_state.append(&mut db, 3).unwrap();
        vec_state.add_all(&mut db, vec![4, 5, 6]).unwrap();

        assert_eq!(vec_state.get(&db).unwrap(), vec![1, 2, 3, 4, 5, 6]);
        //        assert_eq!(vec_state.len(&db).unwrap(), 6);
    }
}
