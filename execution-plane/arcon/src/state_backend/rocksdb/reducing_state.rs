// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use super::rocksdb::{merge_operator::MergeFn, MergeOperands};
use crate::{
    prelude::ArconResult,
    state_backend::{
        rocksdb::{state_common::StateCommon, RocksDb},
        state_types::{AppendingState, MergingState, ReducingState, State},
    },
};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

pub struct RocksDbReducingState<IK, N, T, F> {
    pub(crate) common: StateCommon<IK, N>,

    // here mostly for debugging, we may fall back to it in the future if rocksdb merge proves unreliable
    #[allow(unused)]
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

pub fn make_reducing_merge<T, F>(reduce_fn: F) -> impl MergeFn + Clone
where
    F: Fn(&T, &T) -> T + Send + Sync + Clone + 'static,
    T: Serialize + for<'a> Deserialize<'a>,
{
    move |_key: &[u8], first: Option<&[u8]>, rest: &mut MergeOperands| {
        let res = first
            .into_iter()
            .chain(rest)
            .map(bincode::deserialize)
            .fold_results(None, |acc, value| match acc {
                None => Some(value),
                Some(old) => Some(reduce_fn(&old, &value)),
            });

        // TODO: change eprintlns to actual logs
        // we don't really have a way to send results back to rust across rocksdb ffi, so we just log the errors
        match res {
            Ok(Some(v)) => match bincode::serialize(&v) {
                Ok(serialized) => Some(serialized),
                Err(e) => {
                    eprintln!("reduce state merge result serialization error: {}", e);
                    None
                }
            },
            Ok(None) => {
                eprintln!("reducing state merge result is None???");
                None
            }
            Err(e) => {
                eprintln!("reducing state merge error: {}", e);
                None
            }
        }
    }
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
        let serialized = bincode::serialize(&value)
            .map_err(|e| arcon_err_kind!("Could not serialize reducing state value: {}", e))?;

        let cf = backend.get_cf_handle(&self.common.cf_name)?;
        // See the make_reducing_merge function in this module. Its result is set as the merging operator for this state.
        backend
            .db
            .merge_cf(cf, key, serialized)
            .map_err(|e| arcon_err_kind!("Could not perform merge operation: {}", e))
    }
}

impl<IK, N, T, F> MergingState<RocksDb, IK, N, T, T> for RocksDbReducingState<IK, N, T, F>
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
    use crate::state_backend::{rocksdb::tests::TestDb, ReducingStateBuilder};

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

    #[test]
    fn different_funcs_test() {
        let mut db = TestDb::new();
        let rs1 = db.new_reducing_state("rs1", (), (), |old: &i32, new: &i32| *old.max(new));
        let rs2 = db.new_reducing_state("rs2", (), (), |old: &i32, new: &i32| *old.min(new));

        rs1.append(&mut db, 7).unwrap();
        rs2.append(&mut db, 7).unwrap();
        rs1.append(&mut db, 42).unwrap();
        rs2.append(&mut db, 42).unwrap();
        rs1.append(&mut db, 10).unwrap();
        rs2.append(&mut db, 10).unwrap();

        assert_eq!(rs1.get(&db).unwrap(), 42);
        assert_eq!(rs2.get(&db).unwrap(), 7);
    }
}
