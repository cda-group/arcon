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

pub struct RocksDbReducingState<IK, N, T, F> {
    pub(crate) common: StateCommon<IK, N>,
    pub(crate) reduce_fn: F,
    pub(crate) _phantom: PhantomData<T>,
}

impl<IK, N, T, F> State<RocksDb, IK, N> for RocksDbReducingState<IK, N, T, F>
    where IK: Serialize, N: Serialize {
    fn clear(&self, backend: &mut RocksDb) -> ArconResult<()> {
        let key = self.common.get_db_key(&())?;
        backend.remove(&key)?;
        Ok(())
    }

    delegate_key_and_namespace!(common);
}

impl<IK, N, T, F> AppendingState<RocksDb, IK, N, T, T> for RocksDbReducingState<IK, N, T, F>
// TODO: if we made the (backend-)mutating methods take &mut self, F could be FnMut
    where IK: Serialize, N: Serialize, T: Serialize + for<'a> Deserialize<'a>, F: Fn(&T, &T) -> T {
    fn get(&self, backend: &RocksDb) -> ArconResult<T> {
        let key = self.common.get_db_key(&())?;
        let storage = backend.get(&key)?;
        let value = bincode::deserialize(storage)
            .map_err(|e| arcon_err_kind!("Could not deserialize reducing state value: {}", e))?;

        Ok(value)
    }

    fn append(&self, backend: &mut RocksDb, value: T) -> ArconResult<()> {
        let key = self.common.get_db_key(&())?;
        let storage = backend.get_mut_or_init_empty(&key)?;
        if storage.is_empty() {
            bincode::serialize_into(storage, &value)
                .map_err(|e| arcon_err_kind!("Could not serialize reducing state value: {}", e))?;
            return Ok(());
        }

        let old_value = bincode::deserialize(storage)
            .map_err(|e| arcon_err_kind!("Could not deserialize reducing state value: {}", e))?;

        let new_value = (self.reduce_fn)(&old_value, &value);
        bincode::serialize_into(storage.as_mut_slice(), &new_value)
            .map_err(|e| arcon_err_kind!("Could not serialize reducing state value: {}", e))?;

        Ok(())
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

    #[test]
    fn reducing_state_test() {
        let tmp_dir = TempDir::new().unwrap();
        let dir_path = tmp_dir.path().to_string_lossy().into_owned();
        let mut db = RocksDb::new(&dir_path).unwrap();
        let reducing_state = db.new_reducing_state((), (),
                                                       |old: &i32, new: &i32| *old.max(new));

        reducing_state.append(&mut db, 7).unwrap();
        reducing_state.append(&mut db, 42).unwrap();
        reducing_state.append(&mut db, 10).unwrap();

        assert_eq!(reducing_state.get(&db).unwrap(), 42);
    }
}