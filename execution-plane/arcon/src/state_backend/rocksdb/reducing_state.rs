// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use std::marker::PhantomData;
use serde::{Serialize, Deserialize};
use crate::{
    state_backend::{
        rocksdb::{StateCommon, RocksDb},
        state_types::{State, AppendingState, ReducingState, MergingState},
    },
    prelude::ArconResult,
};
use super::rocksdb::DBPinnableSlice;
use std::error::Error;

pub struct RocksDbReducingState<IK, N, T, F> {
    pub(crate) common: StateCommon<IK, N>,
    pub(crate) reduce_fn: F,
    pub(crate) _phantom: PhantomData<T>,
}

impl<IK, N, T, F> State<RocksDb, IK, N> for RocksDbReducingState<IK, N, T, F>
    where IK: Serialize, N: Serialize {
    fn clear(&self, backend: &mut RocksDb) -> ArconResult<()> {
        let key = self.common.get_db_key(&())?;
        backend.remove(&self.common.cf_name, &key)?;
        Ok(())
    }

    delegate_key_and_namespace!(common);
}

impl<IK, N, T, F> AppendingState<RocksDb, IK, N, T, T> for RocksDbReducingState<IK, N, T, F>
// TODO: if we made the (backend-)mutating methods take &mut self, F could be FnMut
    where IK: Serialize, N: Serialize, T: Serialize + for<'a> Deserialize<'a>, F: Fn(&T, &T) -> T {
    fn get(&self, backend: &RocksDb) -> ArconResult<T> {
        let key = self.common.get_db_key(&())?;
        let storage = backend.get(&self.common.cf_name, &key)?;
        let value = bincode::deserialize(&*storage)
            .map_err(|e| arcon_err_kind!("Could not deserialize reducing state value: {}", e))?;

        Ok(value)
    }

    fn append(&self, backend: &mut RocksDb, value: T) -> ArconResult<()> {
        let key = self.common.get_db_key(&())?;

        // TODO: an implementation driven entirely by rocksdb
        // see the merging operation for reducing state in RocksDb::create_cf_options_for
//        let cf = backend.get_cf_handle(&self.common.cf_name)?;
//        backend.db.merge_cf(cf, key, serialized)
//            .map_err(|e| arcon_err_kind!("Could not perform merge operation: {}", e))?;

        // TODO: this impl is not thread safe, values in rocksdb can be unexpectedly ignored
        let old_value_opt = match backend.get(&self.common.cf_name, &key) {
            Ok(serialized) => {
                Some(bincode::deserialize(&*serialized)
                    .map_err(|e| arcon_err_kind!("Couldn't deserialize reducing state value: {}", e))?)
            }
            Err(e) if e.to_string().contains("Value not found") => { // TODO: proper error enums would be great
                None
            }
            Err(e) => {
                return Err(e);
            }
        };

        match old_value_opt {
            None => {
                let new_serialized = bincode::serialize(&value)
                    .map_err(|e| arcon_err_kind!("Could not serialize reducing state value: {}", e))?;
                backend.put(&self.common.cf_name, key, new_serialized)?;
                Ok(())
            }
            Some(old_value) => {
                let new_value = (self.reduce_fn)(&old_value, &value);
                let new_serialized = bincode::serialize(&new_value)
                    .map_err(|e| arcon_err_kind!("Could not serialize reducing state value: {}", e))?;
                backend.put(&self.common.cf_name, key, new_serialized)?;
                Ok(())
            }
        }
    }
}

impl<IK, N, T, F> MergingState<RocksDb, IK, N, T, T> for RocksDbReducingState<IK, N, T, F>
// TODO: if we made the (backend-)mutating methods take &mut self, F could be FnMut
    where IK: Serialize, N: Serialize, T: Serialize + for<'a> Deserialize<'a>, F: Fn(&T, &T) -> T {}

impl<IK, N, T, F> ReducingState<RocksDb, IK, N, T> for RocksDbReducingState<IK, N, T, F>
    where IK: Serialize, N: Serialize, T: Serialize + for<'a> Deserialize<'a>, F: Fn(&T, &T) -> T {}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::{StateBackend, ReducingStateBuilder};
    use crate::state_backend::rocksdb::tests::TestDb;

    #[test]
    fn reducing_state_test() {
        let mut db = TestDb::new();
        let reducing_state = db.new_reducing_state("test_state", (), (), |old: &i32, new: &i32| *old.max(new));

        reducing_state.append(&mut db, 7).unwrap();
        reducing_state.append(&mut db, 42).unwrap();
        reducing_state.append(&mut db, 10).unwrap();

        assert_eq!(reducing_state.get(&db).unwrap(), 42);
    }
}