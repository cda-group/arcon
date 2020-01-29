// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

extern crate rocksdb;

use arcon_error::*;
use serde::{Serialize, Deserialize};
use rocksdb::{
    checkpoint::Checkpoint,
    DB,
    ColumnFamilyDescriptor,
    Options,
    ColumnFamily,
    DBPinnableSlice,
    WriteBatch,
};
use std::{
    path::{PathBuf, Path},
    io::Write,
    error::Error,
};
use crate::state_backend::{StateBackend, ValueStateBuilder, MapStateBuilder, rocksdb::{
    value_state::RocksDbValueState,
    map_state::RocksDbMapState,
}, state_types::*, VecStateBuilder, ReducingStateBuilder};
use self::rocksdb::{SliceTransform, MergeOperands};
use self::rocksdb::merge_operator::MergeFn;
use crate::state_backend::rocksdb::vec_state::RocksDbVecState;
use crate::state_backend::rocksdb::reducing_state::RocksDbReducingState;

pub struct RocksDb {
    db: DB,
    checkpoints_path: PathBuf,
}

impl RocksDb {
    fn get_cf_handle(&self, cf_name: impl AsRef<str>) -> ArconResult<&ColumnFamily> {
        self.db.cf_handle(cf_name.as_ref())
            .ok_or_else(|| arcon_err_kind!("Could not get column family '{}'", cf_name.as_ref()))
    }

    fn set_checkpoints_path<P: AsRef<Path>>(&mut self, path: P) {
        self.checkpoints_path = path.as_ref().to_path_buf();
    }

    fn get(&self, cf_name: impl AsRef<str>, key: impl AsRef<[u8]>) -> ArconResult<DBPinnableSlice> {
        let cf = self.get_cf_handle(cf_name)?;

        match self.db.get_pinned_cf(cf, key) {
            Ok(Some(data)) => Ok(data),
            Ok(None) => arcon_err!("Value not found"),
            Err(e) => arcon_err!("Could not get map state value: {}", e),
        }
    }

    fn put(&mut self, cf_name: impl AsRef<str>, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> ArconResult<()> {
        let cf = self.get_cf_handle(cf_name)?;

        self.db.put_cf(cf, key, value)
            .map_err(|e| arcon_err_kind!("RocksDB put err: {}", e))
    }

    fn remove(&mut self, cf: impl AsRef<str>, key: impl AsRef<[u8]>) -> ArconResult<()> {
        let cf = self.get_cf_handle(cf)?;
        self.db.delete_cf(cf, key)
            .map_err(|e| arcon_err_kind!("RocksDB delete err: {}", e))
    }

    fn remove_prefix(&mut self, cf: impl AsRef<str>, cf_opts: &Options, prefix: impl AsRef<[u8]>) -> ArconResult<()> {
        // We use DB::delete_range_cf here, which should be faster than what Flink does, because it
        // doesn't require explicit iteration. BUT! it assumes that the prefixes have constant
        // length, i.e. IK and N always bincode serialize to the same number of bytes.
        // TODO: fix that? or restrict possible item-key and namespace types

        let prefix = prefix.as_ref();
        let cf_name = cf.as_ref();

        if prefix.is_empty() {
            // prefix is empty, so we use the fast path of dropping and re-creating the whole
            // column family
            self.db.drop_cf(cf_name)
                .map_err(|e| arcon_err_kind!("Could not drop column family '{}': {}", cf_name, e))?;

            self.db.create_cf(cf_name, &cf_opts)
                .map_err(|e| arcon_err_kind!("Could not recreate column family '{}': {}", cf_name, e))?;

            return Ok(());
        }

        let cf = self.get_cf_handle(cf_name)?;

        // NOTE: this only works assuming the column family is lexicographically ordered (which is
        // the default, so we don't explicitly set it, see Options::set_comparator)
        let start = prefix;
        // delete_range deletes all the entries in [start, end) range, so we can just increment the
        // least significant byte of the prefix
        let mut end = start.to_vec();
        *end.last_mut().expect("unreachable, the empty case is covered a few lines above") += 1;

        let mut wb = WriteBatch::default();
        wb.delete_range_cf(cf, start, &end)
            .map_err(|e| arcon_err_kind!("Could not create delete_range operation: {}", e))?;

        self.db.write(wb)
            .map_err(|e| arcon_err_kind!("Could not perform delete_range operation: {}", e))?;

        Ok(())
    }

    fn contains(&self, cf: impl AsRef<str>, key: impl AsRef<[u8]>) -> ArconResult<bool> {
        let cf = self.get_cf_handle(cf.as_ref())?;
        Ok(self.db.get_pinned_cf(cf, key)
            .map_err(|e| arcon_err_kind!("Could not get map state value: {}", e))?
            .is_some())
    }

    fn get_or_create_column_family<IK: Serialize, N: Serialize>(
        &mut self,
        state_name: &str,
        state_type: StateType,
        item_key: &IK,
        namespace: &N,
    ) -> (String, Options) {
        let opts = self.create_cf_options_for(state_type, item_key, namespace)
            .expect("Could not create options for new column family"); // TODO: propagate as Result?

        // Every state has its own column family. Different state types have potentially different
        // column family options. TODO: Q: is that the optimal solution?
        if self.db.cf_handle(state_name).is_none() {
            self.db.create_cf(state_name, &opts);
        }

        (state_name.to_string(), opts)
    }

    fn create_state_common<IK: Serialize, N: Serialize>(
        &mut self,
        state_name: &str,
        state_type: StateType,
        item_key: IK,
        namespace: N,
    ) -> StateCommon<IK, N> {
        let (cf_name, cf_options) = self.get_or_create_column_family(state_name, state_type, &item_key, &namespace);
        StateCommon { cf_name, cf_options, item_key, namespace }
    }

    fn create_cf_options_for<IK: Serialize, N: Serialize>(&self, state_type: StateType, item_key: &IK, namespace: &N) -> ArconResult<Options> {
        // The line below should yield the same size for any values of given types IK, N. This is
        // not enforced anywhere yet, but we rely on it. For example, neither IK nor N should be
        // Vec<T> or String, because those types serialize to variable length byte arrays.
        // TODO: restrict possible IK and N with a trait? We could add an associated const there,
        //  so the computation below is eliminated.
        let prefix_size = bincode::serialized_size(&(item_key, namespace))
            .map_err(|e| arcon_err_kind!("Couldn't compute prefix size for column family: {}", e))?;

        // base opts
        let mut opts = Options::default();
        // for map state to work properly, but useful for all the states, so the bloom filters get
        // populated
        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(prefix_size as usize));

        match state_type {
            StateType::MapState => {}
            StateType::ValueState => {}
            StateType::VecState => {
                let concat_merge: MergeFn = |_key, first, rest| {
                    let mut result: Vec<u8> = Vec::with_capacity(rest.size_hint().0);
                    first.map(|v| {
                        result.extend_from_slice(v);
                    });
                    for op in rest {
                        result.extend_from_slice(op);
                    }
                    Some(result)
                };
                opts.set_merge_operator("concat_merge", concat_merge, None);
            }
            StateType::ReducingState => {
                let reduce_merge: MergeFn = |_key, first, rest| {
                    let all_values_iter = first.into_iter().chain(rest);
                    todo!("running reduce_fn by rocksdb not yet supported")
                };
//                opts.set_merge_operator("reduce_merge", reduce_merge, None);
            }
            StateType::AggregatingState => {
                let aggregate_merge: MergeFn = |_key, first, rest| {
                    let all_values_iter = first.into_iter().chain(rest);
                    todo!("running aggregator by rocksdb not yet supported")
                };
//                opts.set_merge_operator("aggregate_merge", aggregate_merge, None);
            }
        }

        Ok(opts)
    }
}

impl StateBackend for RocksDb {
    fn new(name: &str) -> ArconResult<RocksDb> {
        // those are database options, cf options come from RocksDb::create_db_options_for
        let mut opts = Options::default();
        opts.create_if_missing(true);

        // TODO: maybe we need multiple listings with different options???
        let column_families = match DB::list_cf(&opts, &name) {
            Ok(cfs) => { cfs }
            // TODO: possibly platform-dependant error message check
            Err(e) if e.description().contains("No such file or directory") => { vec!["default".to_string()] }
            Err(e) => { return arcon_err!("Could not list column families: {}", e); }
        };

        let db = DB::open_cf(&opts, &name, column_families)
            .map_err(|e| arcon_err_kind!("Failed to create RocksDB instance: {}", e))?;

        let checkpoints_path = PathBuf::from(format!("{}-checkpoints", name));

        Ok(RocksDb { db, checkpoints_path })
    }

    fn checkpoint(&self, id: String) -> ArconResult<()> {
        let checkpointer = Checkpoint::new(&self.db)
            .map_err(|e| arcon_err_kind!("Could not create checkpoint object: {}", e))?;

        let mut path = self.checkpoints_path.clone();
        path.push(id);

        self.db.flush()
            .map_err(|e| arcon_err_kind!("Could not flush rocksdb: {}", e))?;

        checkpointer.create_checkpoint(path)
            .map_err(|e| arcon_err_kind!("Could not save the checkpoint: {}", e))?;
        Ok(())
    }
}

impl<IK, N, T> ValueStateBuilder<IK, N, T> for RocksDb
    where IK: Serialize, N: Serialize, T: Serialize + for<'a> Deserialize<'a> {
    type Type = RocksDbValueState<IK, N, T>;

    fn new_value_state(&mut self, name: &str, init_item_key: IK, init_namespace: N) -> Self::Type {
        let common = self.create_state_common(name, Self::Type::TYPE, init_item_key, init_namespace);
        RocksDbValueState { common, _phantom: Default::default() }
    }
}

impl<IK, N, K, V> MapStateBuilder<IK, N, K, V> for RocksDb
    where
        IK: Serialize + for<'a> Deserialize<'a>,
        N: Serialize + for<'a> Deserialize<'a>,
        K: Serialize + for<'a> Deserialize<'a>,
        V: Serialize + for<'a> Deserialize<'a> {
    type Type = RocksDbMapState<IK, N, K, V>;

    fn new_map_state(&mut self, name: &str, init_item_key: IK, init_namespace: N) -> Self::Type {
        let common = self.create_state_common(name, Self::Type::TYPE, init_item_key, init_namespace);
        RocksDbMapState { common, _phantom: Default::default() }
    }
}

impl<IK, N, T> VecStateBuilder<IK, N, T> for RocksDb
    where IK: Serialize, N: Serialize, T: Serialize + for<'a> Deserialize<'a> {
    type Type = RocksDbVecState<IK, N, T>;

    fn new_vec_state(&mut self, name: &str, init_item_key: IK, init_namespace: N) -> Self::Type {
        let common = self.create_state_common(name, Self::Type::TYPE, init_item_key, init_namespace);
        RocksDbVecState { common, _phantom: Default::default() }
    }
}

impl<IK, N, T, F> ReducingStateBuilder<IK, N, T, F> for RocksDb
    where IK: Serialize, N: Serialize, T: Serialize + for<'a> Deserialize<'a>, F: Fn(&T, &T) -> T {
    type Type = RocksDbReducingState<IK, N, T, F>;

    fn new_reducing_state(&mut self, name: &str, init_item_key: IK, init_namespace: N, reduce_fn: F) -> Self::Type {
        let common = self.create_state_common(name, Self::Type::TYPE, init_item_key, init_namespace);
        RocksDbReducingState { common, reduce_fn, _phantom: Default::default() }
    }
}

pub(crate) struct StateCommon<IK, N> {
    cf_name: String,
    cf_options: Options,
    item_key: IK,
    namespace: N,
}

impl<IK, N> StateCommon<IK, N> where IK: Serialize, N: Serialize {
    fn get_db_key<UK>(&self, user_key: &UK) -> ArconResult<Vec<u8>> where UK: Serialize {
        let res = bincode::serialize(&(
            &self.item_key,
            &self.namespace,
            user_key
        )).map_err(|e| arcon_err_kind!("Could not serialize keys and namespace: {}", e))?;

        Ok(res)
    }
}

mod value_state;
mod map_state;
mod vec_state;
mod reducing_state;
//mod aggregating_state; TODO

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::ops::{Deref, DerefMut};

    pub struct TestDb {
        db: RocksDb,
        dir: TempDir,
    }

    impl TestDb {
        pub fn new() -> TestDb {
            let dir = TempDir::new().unwrap();
            let dir_path = dir.path().to_string_lossy().into_owned();
            let mut db = RocksDb::new(&dir_path).unwrap();
            TestDb { db, dir }
        }
    }

    impl Deref for TestDb {
        type Target = RocksDb;

        fn deref(&self) -> &Self::Target {
            &self.db
        }
    }

    impl DerefMut for TestDb {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.db
        }
    }


    #[test]
    fn test_unit_state_key_empty() {
        let state = StateCommon {
            cf_name: "".to_string(),
            cf_options: Options::default(),
            item_key: (),
            namespace: (),
        };

        let v = state.get_db_key(&()).unwrap();

        assert!(v.is_empty());
    }

    #[test]
    fn simple_rocksdb_test() {
        let mut db = TestDb::new();

        let key = "key";
        let value = "test";
        let column_family = "default";

        db.put(column_family, key.as_bytes(), value.as_bytes()).expect("put");

        {
            let v = db.get(column_family, key.as_bytes()).unwrap();
            assert_eq!(value, String::from_utf8_lossy(&v));
        }

        db.remove(column_family, key.as_bytes()).expect("remove");
        let v = db.get(column_family, key.as_bytes());
        assert!(v.is_err());
    }

    #[test]
    fn checkpoint_rocksdb_test() {
        let tmp_dir = TempDir::new().unwrap();
        let checkpoints_dir = TempDir::new().unwrap();

        let dir_path = tmp_dir.path().to_string_lossy();
        let checkpoints_dir_path = checkpoints_dir.path().to_string_lossy();

        let mut db = RocksDb::new(&dir_path).unwrap();
        db.set_checkpoints_path(checkpoints_dir_path.as_ref());

        let key: &[u8] = b"key";
        let initial_value: &[u8] = b"value";
        let new_value: &[u8] = b"new value";
        let column_family = "default";

        db.put(column_family, key, initial_value).expect("put failed");
        db.checkpoint("chkpt0".into()).expect("checkpoint failed");
        db.put(column_family, key, new_value).expect("second put failed");

        let mut last_checkpoint_path = checkpoints_dir.path().to_owned();
        last_checkpoint_path.push("chkpt0");

        let db_from_checkpoint = RocksDb::new(&last_checkpoint_path.to_string_lossy())
            .expect("Could not open checkpointed db");

        assert_eq!(
            new_value,
            db.get(column_family, key)
                .expect("Could not get from the original db")
                .as_ref()
        );
        assert_eq!(
            initial_value,
            db_from_checkpoint.get(column_family, key)
                .expect("Could not get from the checkpoint")
                .as_ref()
        );
    }
}
