// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use super::rocksdb::MergeOperands;
use crate::{
    prelude::ArconResult,
    state_backend::{
        rocksdb::{state_common::StateCommon, RocksDb},
        state_types::{AppendingState, MergingState, State, VecState},
    },
};
use error::ErrorKind;
use rocksdb::WriteBatch;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

pub struct RocksDbVecState<IK, N, T> {
    pub(crate) common: StateCommon<IK, N>,
    pub(crate) _phantom: PhantomData<T>,
}

impl<IK, N, T> State<RocksDb, IK, N> for RocksDbVecState<IK, N, T>
where
    IK: Serialize,
    N: Serialize,
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

impl<IK, N, T> AppendingState<RocksDb, IK, N, T, Vec<T>> for RocksDbVecState<IK, N, T>
where
    IK: Serialize,
    N: Serialize,
    T: Serialize,
    T: for<'a> Deserialize<'a>,
{
    fn get(&self, backend: &RocksDb) -> ArconResult<Vec<T>> {
        let key = self.common.get_db_key(&())?;
        let serialized = backend.get(&self.common.cf_name, &key)?;

        // reader is updated in the loop to point at the yet unconsumed part of the serialized data
        let mut reader = &serialized[..];
        let mut res = vec![];
        while !reader.is_empty() {
            let val = bincode::deserialize_from(&mut reader)
                .map_err(|e| arcon_err_kind!("Could not deserialize vec state value: {}", e))?;
            res.push(val);
        }

        Ok(res)
    }

    fn append(&self, backend: &mut RocksDb, value: T) -> ArconResult<()> {
        let backend = backend.initialized_mut()?;

        let key = self.common.get_db_key(&())?;
        let serialized = bincode::serialize(&value)
            .map_err(|e| arcon_err_kind!("Could not serialize vec state value: {}", e))?;

        let cf = backend.get_cf_handle(&self.common.cf_name)?;
        // See the vec_merge function in this module. It is set as the merge operator for every vec state.
        backend
            .db
            .merge_cf(cf, key, serialized)
            .map_err(|e| arcon_err_kind!("Could not perform merge operation: {}", e))
    }
}

impl<IK, N, T> MergingState<RocksDb, IK, N, T, Vec<T>> for RocksDbVecState<IK, N, T>
where
    IK: Serialize,
    N: Serialize,
    T: Serialize,
    T: for<'a> Deserialize<'a>,
{
}

impl<IK, N, T> VecState<RocksDb, IK, N, T> for RocksDbVecState<IK, N, T>
where
    IK: Serialize,
    N: Serialize,
    T: Serialize,
    T: for<'a> Deserialize<'a>,
{
    fn set(&self, backend: &mut RocksDb, value: Vec<T>) -> ArconResult<()> {
        let key = self.common.get_db_key(&())?;
        let mut storage = vec![];
        for elem in value {
            bincode::serialize_into(&mut storage, &elem)
                .map_err(|e| arcon_err_kind!("Could not serialize vec state value: {}", e))?;
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
            let serialized = bincode::serialize(&value)
                .map_err(|e| arcon_err_kind!("Could not serialize vec state value: {}", e))?;

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

    fn len(&self, backend: &RocksDb) -> ArconResult<usize> {
        let key = self.common.get_db_key(&())?;
        let storage = backend.get(&self.common.cf_name, &key);

        match storage {
            Err(e) => match e.kind() {
                ErrorKind::ArconError(message) if &*message == "Value not found" => Ok(0),
                _ => Err(e),
            },
            Ok(buf) => {
                let mut reader = &*buf;
                if buf.is_empty() {
                    return Ok(0);
                }

                // We rely on every possible value of type T having the same serialized size.
                // TODO: introduce a trait for types that serialize to a fixed-size byte array and
                //   add that as a trait bound on T
                let first_value: T = bincode::deserialize_from(&mut reader)
                    .map_err(|e| arcon_err_kind!("Could not deserialize vec state value: {}", e))?;
                let first_value_serialized_size =
                    bincode::serialized_size(&first_value).map_err(|e| {
                        arcon_err_kind!(
                            "Could not get the size of serialized vec state value: {}",
                            e
                        )
                    })? as usize;

                debug_assert_ne!(first_value_serialized_size, 0);

                let len = buf.len() / first_value_serialized_size;
                let rem = buf.len() % first_value_serialized_size;

                // sanity check
                if rem != 0 {
                    return arcon_err!(
                        "vec state storage length is not a multiple of element size"
                    );
                }

                Ok(len)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::{rocksdb::tests::TestDb, VecStateBuilder};

    #[test]
    fn vec_state_test() {
        let mut db = TestDb::new();
        let vec_state = db.new_vec_state("test_state", (), ());
        assert_eq!(vec_state.len(&db).unwrap(), 0);

        vec_state.append(&mut db, 1).unwrap();
        vec_state.append(&mut db, 2).unwrap();
        vec_state.append(&mut db, 3).unwrap();
        vec_state.add_all(&mut db, vec![4, 5, 6]).unwrap();

        assert_eq!(vec_state.get(&db).unwrap(), vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(vec_state.len(&db).unwrap(), 6);
    }
}
