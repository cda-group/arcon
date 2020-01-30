// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::ArconResult,
    state_backend::{
        rocksdb::{state_common::StateCommon, RocksDb},
        state_types::{AppendingState, MergingState, ReducingState, State},
    },
};
use rocksdb::DBPinnableSlice;
use serde::{Deserialize, Serialize};
use std::{error::Error, marker::PhantomData};

pub struct RocksDbReducingState<IK, N, T, F> {
    pub(crate) common: StateCommon<IK, N>,
    pub(crate) reduce_fn: F,
    pub(crate) _phantom: PhantomData<T>,
}

impl<IK, N, T, F> State<RocksDb, IK, N> for RocksDbReducingState<IK, N, T, F>
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

impl<IK, N, T, F> AppendingState<RocksDb, IK, N, T, T> for RocksDbReducingState<IK, N, T, F>
// TODO: if we made the (backend-)mutating methods take &mut self, F could be FnMut
where
    IK: Serialize,
    N: Serialize,
    T: Serialize + for<'a> Deserialize<'a>,
    F: Fn(&T, &T) -> T,
{
    fn get(&self, backend: &RocksDb) -> ArconResult<T> {
        let key = self.common.get_db_key(&())?;
        let storage = backend.get(&self.common.cf_name, &key)?;
        let value = bincode::deserialize(&*storage)
            .map_err(|e| arcon_err_kind!("Could not deserialize reducing state value: {}", e))?;

        Ok(value)
    }

    fn append(&self, backend: &mut RocksDb, value: T) -> ArconResult<()> {
        let key = self.common.get_db_key(&())?;

        // see the merging operation for reducing state in StateCommon::new_for_reducing_state
        let cf = backend.get_cf_handle(&self.common.cf_name)?;
        backend
            .db
            .merge_cf(cf, key, serialized)
            .map_err(|e| arcon_err_kind!("Could not perform merge operation: {}", e))
    }
}

impl<IK, N, T, F> MergingState<RocksDb, IK, N, T, T> for RocksDbReducingState<IK, N, T, F>
// TODO: if we made the (backend-)mutating methods take &mut self, F could be FnMut
where
    IK: Serialize,
    N: Serialize,
    T: Serialize + for<'a> Deserialize<'a>,
    F: Fn(&T, &T) -> T,
{
}

impl<IK, N, T, F> ReducingState<RocksDb, IK, N, T> for RocksDbReducingState<IK, N, T, F>
where
    IK: Serialize,
    N: Serialize,
    T: Serialize + for<'a> Deserialize<'a>,
    F: Fn(&T, &T) -> T,
{
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::{rocksdb::tests::TestDb, ReducingStateBuilder, StateBackend};

    #[test]
    fn reducing_state_test() {
        let mut db = TestDb::new();
        let reducing_state =
            db.new_reducing_state("test_state", (), (), |old: &i32, new: &i32| *old.max(new));

        reducing_state.append(&mut db, 7).unwrap();
        reducing_state.append(&mut db, 42).unwrap();
        reducing_state.append(&mut db, 10).unwrap();

        assert_eq!(reducing_state.get(&db).unwrap(), 42);
    }
}
