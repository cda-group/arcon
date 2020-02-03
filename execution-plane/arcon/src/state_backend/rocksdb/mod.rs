// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

extern crate rocksdb;

use self::rocksdb::ColumnFamilyDescriptor;
use crate::state_backend::{
    rocksdb::{
        aggregating_state::RocksDbAggregatingState, map_state::RocksDbMapState,
        reducing_state::RocksDbReducingState, state_common::StateCommon,
        value_state::RocksDbValueState, vec_state::RocksDbVecState,
    },
    state_types::*,
    AggregatingStateBuilder, MapStateBuilder, ReducingStateBuilder, StateBackend,
    ValueStateBuilder, VecStateBuilder,
};
use arcon_error::*;
use rocksdb::{
    checkpoint::Checkpoint, ColumnFamily, DBPinnableSlice, Options, SliceTransform, WriteBatch, DB,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    iter::FromIterator,
    mem,
    path::{Component, Path, PathBuf},
};

pub struct RocksDb {
    inner: Inner,
    path: PathBuf,
}

enum Inner {
    Initialized(InitializedRocksDb),
    Uninitialized {
        unknown_cfs: HashSet<String>,
        known_cfs: HashMap<String, Options>,
    },
}

pub struct InitializedRocksDb {
    db: DB,
    // this is here so we can easily clear the column family by dropping and recreating it
    options: HashMap<String, Options>,
}

impl InitializedRocksDb {
    fn get_cf_handle(&self, cf_name: impl AsRef<str>) -> ArconResult<&ColumnFamily> {
        self.db
            .cf_handle(cf_name.as_ref())
            .ok_or_else(|| arcon_err_kind!("Could not get column family '{}'", cf_name.as_ref()))
    }

    fn get(&self, cf_name: impl AsRef<str>, key: impl AsRef<[u8]>) -> ArconResult<DBPinnableSlice> {
        let cf = self.get_cf_handle(cf_name)?;

        match self.db.get_pinned_cf(cf, key) {
            Ok(Some(data)) => Ok(data),
            Ok(None) => arcon_err!("Value not found"),
            Err(e) => arcon_err!("Could not get map state value: {}", e),
        }
    }

    fn put(
        &mut self,
        cf_name: impl AsRef<str>,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> ArconResult<()> {
        let cf = self.get_cf_handle(cf_name)?;

        self.db
            .put_cf(cf, key, value)
            .map_err(|e| arcon_err_kind!("RocksDB put err: {}", e))
    }

    fn remove(&mut self, cf: impl AsRef<str>, key: impl AsRef<[u8]>) -> ArconResult<()> {
        let cf = self.get_cf_handle(cf)?;
        self.db
            .delete_cf(cf, key)
            .map_err(|e| arcon_err_kind!("RocksDB delete err: {}", e))
    }

    fn remove_prefix(&mut self, cf: impl AsRef<str>, prefix: impl AsRef<[u8]>) -> ArconResult<()> {
        // We use DB::delete_range_cf here, which should be faster than what Flink does, because it
        // doesn't require explicit iteration. BUT! it assumes that the prefixes have constant
        // length, i.e. IK and N always serialize to the same number of bytes.
        // TODO: fix that? or restrict possible item-key and namespace types

        let prefix = prefix.as_ref();
        let cf_name = cf.as_ref();

        if prefix.is_empty() {
            // prefix is empty, so we use the fast path of dropping and re-creating the whole
            // column family

            let cf_opts = &self.options[cf_name];

            self.db.drop_cf(cf_name).map_err(|e| {
                arcon_err_kind!("Could not drop column family '{}': {}", cf_name, e)
            })?;

            self.db.create_cf(cf_name, cf_opts).map_err(|e| {
                arcon_err_kind!("Could not recreate column family '{}': {}", cf_name, e)
            })?;

            return Ok(());
        }

        let cf = self.get_cf_handle(cf_name)?;

        // NOTE: this only works assuming the column family is lexicographically ordered (which is
        // the default, so we don't explicitly set it, see Options::set_comparator)
        let start = prefix;
        // delete_range deletes all the entries in [start, end) range, so we can just increment the
        // least significant byte of the prefix
        let mut end = start.to_vec();
        *end.last_mut()
            .expect("unreachable, the empty case is covered a few lines above") += 1;

        let mut wb = WriteBatch::default();
        wb.delete_range_cf(cf, start, &end)
            .map_err(|e| arcon_err_kind!("Could not create delete_range operation: {}", e))?;

        self.db
            .write(wb)
            .map_err(|e| arcon_err_kind!("Could not perform delete_range operation: {}", e))?;

        Ok(())
    }

    fn contains(&self, cf: impl AsRef<str>, key: impl AsRef<[u8]>) -> ArconResult<bool> {
        let cf = self.get_cf_handle(cf.as_ref())?;
        Ok(self
            .db
            .get_pinned_cf(cf, key)
            .map_err(|e| arcon_err_kind!("Could not get map state value: {}", e))?
            .is_some())
    }

    fn create_column_family_if_doesnt_exist(&mut self, cf_name: &str, opts: Options) {
        if self.db.cf_handle(cf_name).is_none() {
            self.db
                .create_cf(cf_name, &opts)
                .expect("Could not create column family"); // TODO: propagate
            self.options.insert(cf_name.into(), opts);
        }
    }
}

impl RocksDb {
    #[inline]
    fn initialized(&self) -> ArconResult<&InitializedRocksDb> {
        match &self.inner {
            Inner::Initialized(i) => Ok(i),
            Inner::Uninitialized { unknown_cfs, .. } => arcon_err!(
                "Database not initialized, missing cf descriptors: {:?}",
                unknown_cfs
            ),
        }
    }

    #[inline]
    fn initialized_mut(&mut self) -> ArconResult<&mut InitializedRocksDb> {
        match &mut self.inner {
            Inner::Initialized(i) => Ok(i),
            Inner::Uninitialized { unknown_cfs, .. } => arcon_err!(
                "Database not initialized, missing cf descriptors: {:?}",
                unknown_cfs
            ),
        }
    }

    #[inline]
    fn get(&self, cf_name: impl AsRef<str>, key: impl AsRef<[u8]>) -> ArconResult<DBPinnableSlice> {
        self.initialized()?.get(cf_name, key)
    }

    #[inline]
    fn put(
        &mut self,
        cf_name: impl AsRef<str>,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> ArconResult<()> {
        self.initialized_mut()?.put(cf_name, key, value)
    }

    #[inline]
    fn remove(&mut self, cf_name: impl AsRef<str>, key: impl AsRef<[u8]>) -> ArconResult<()> {
        self.initialized_mut()?.remove(cf_name, key)
    }

    #[inline]
    fn remove_prefix(&mut self, cf: impl AsRef<str>, prefix: impl AsRef<[u8]>) -> ArconResult<()> {
        self.initialized_mut()?.remove_prefix(cf, prefix)
    }

    #[inline]
    fn contains(&self, cf: impl AsRef<str>, key: impl AsRef<[u8]>) -> ArconResult<bool> {
        self.initialized()?.contains(cf, key)
    }

    fn get_or_create_column_family(&mut self, cf_name: &str, opts: Options) -> String {
        match &mut self.inner {
            Inner::Initialized(i) => i.create_column_family_if_doesnt_exist(cf_name, opts),
            Inner::Uninitialized {
                unknown_cfs,
                known_cfs,
            } => {
                if known_cfs.contains_key(cf_name) {
                    return cf_name.to_string();
                }

                unknown_cfs.remove(cf_name);
                known_cfs.insert(cf_name.to_string(), opts);

                let all_cfs_known = if let Inner::Uninitialized { unknown_cfs, .. } = &self.inner {
                    unknown_cfs.is_empty()
                } else {
                    false
                };

                if all_cfs_known {
                    self.initialize().expect("Could not initialize"); // TODO: propagate
                }
            }
        }

        cf_name.to_string()
    }

    fn initialize(&mut self) -> ArconResult<()> {
        assert!(!self.is_initialized());
        let options = {
            let (no_unknown_cfs, known_cfs_ref) = match &mut self.inner {
                Inner::Uninitialized {
                    unknown_cfs,
                    known_cfs,
                } => (unknown_cfs.is_empty(), known_cfs),
                Inner::Initialized { .. } => unreachable!(),
            };
            assert!(no_unknown_cfs);
            let mut known_cfs = HashMap::new();
            mem::swap(known_cfs_ref, &mut known_cfs);
            known_cfs
        };

        let mut whole_db_opts: Options = Default::default();
        whole_db_opts.create_if_missing(true);

        let cfds: Vec<_> = options
            .into_iter()
            .map(|(name, opts)| ColumnFamilyDescriptor::new(name, opts))
            .collect();

        let mut new_inner = Inner::Initialized(InitializedRocksDb {
            db: DB::open_cf_descriptors_borrowed(&whole_db_opts, &self.path, &cfds)
                .map_err(|e| arcon_err_kind!("Couldn't open db: {}", e))?,
            options: cfds
                .into_iter()
                .map(|cfd| (cfd.name, cfd.options))
                .collect(),
        });

        mem::swap(&mut self.inner, &mut new_inner);
        Ok(())
    }

    #[inline]
    fn is_initialized(&self) -> bool {
        match &self.inner {
            Inner::Initialized { .. } => true,
            Inner::Uninitialized { .. } => false,
        }
    }
}

impl StateBackend for RocksDb {
    fn new(path_str: &str) -> ArconResult<RocksDb> {
        // those are database options, cf options come from RocksDb::create_db_options_for
        let mut opts = Options::default();
        opts.create_if_missing(true);

        let path: PathBuf = path_str.into();
        let path = path
            .canonicalize()
            .map_err(|e| arcon_err_kind!("Cannot canonicalize path '{}': {}", path_str, e))?;

        // TODO: maybe we need multiple listings with different options???
        let column_families: HashSet<String> = match DB::list_cf(&opts, &path) {
            Ok(cfs) => cfs.into_iter().filter(|n| n != "default").collect(),
            // TODO: possibly platform-dependant error message check
            Err(e) if e.description().contains("No such file or directory") => HashSet::new(),
            Err(e) => {
                return arcon_err!("Could not list column families: {}", e);
            }
        };

        let inner = if !column_families.is_empty() {
            Inner::Uninitialized {
                unknown_cfs: column_families,
                known_cfs: HashMap::from_iter(std::iter::once((
                    "default".to_string(),
                    Options::default(),
                ))),
            }
        } else {
            let cfds = vec![ColumnFamilyDescriptor::new("default", Options::default())];

            Inner::Initialized(InitializedRocksDb {
                db: DB::open_cf_descriptors_borrowed(&opts, &path, &cfds)
                    .map_err(|e| arcon_err_kind!("Couldn't open DB: {}", e))?,
                options: cfds
                    .into_iter()
                    .map(|cfd| (cfd.name, cfd.options))
                    .collect(),
            })
        };

        Ok(RocksDb { inner, path })
    }

    fn checkpoint(&self, checkpoint_path: String) -> ArconResult<()> {
        let InitializedRocksDb { db, .. } = self.initialized()?;

        db.flush()
            .map_err(|e| arcon_err_kind!("Could not flush rocksdb: {}", e))?;

        let checkpointer = Checkpoint::new(db)
            .map_err(|e| arcon_err_kind!("Could not create checkpoint object: {}", e))?;

        checkpointer
            .create_checkpoint(checkpoint_path)
            .map_err(|e| arcon_err_kind!("Could not save the checkpoint: {}", e))?;
        Ok(())
    }

    fn restore(checkpoint_path: &str, restore_path: &str) -> ArconResult<Self>
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

impl<IK, N, T> ValueStateBuilder<IK, N, T> for RocksDb
where
    IK: Serialize,
    N: Serialize,
    T: Serialize + for<'a> Deserialize<'a>,
{
    type Type = RocksDbValueState<IK, N, T>;

    fn new_value_state(&mut self, name: &str, init_item_key: IK, init_namespace: N) -> Self::Type {
        let common = StateCommon::new_for_value_state(self, name, init_item_key, init_namespace);
        RocksDbValueState {
            common,
            _phantom: Default::default(),
        }
    }
}

impl<IK, N, K, V> MapStateBuilder<IK, N, K, V> for RocksDb
where
    IK: Serialize + for<'a> Deserialize<'a>,
    N: Serialize + for<'a> Deserialize<'a>,
    K: Serialize + for<'a> Deserialize<'a>,
    V: Serialize + for<'a> Deserialize<'a>,
{
    type Type = RocksDbMapState<IK, N, K, V>;

    fn new_map_state(&mut self, name: &str, init_item_key: IK, init_namespace: N) -> Self::Type {
        let common = StateCommon::new_for_map_state(self, name, init_item_key, init_namespace);
        RocksDbMapState {
            common,
            _phantom: Default::default(),
        }
    }
}

impl<IK, N, T> VecStateBuilder<IK, N, T> for RocksDb
where
    IK: Serialize,
    N: Serialize,
    T: Serialize + for<'a> Deserialize<'a>,
{
    type Type = RocksDbVecState<IK, N, T>;

    fn new_vec_state(&mut self, name: &str, init_item_key: IK, init_namespace: N) -> Self::Type {
        let common = StateCommon::new_for_vec_state(self, name, init_item_key, init_namespace);
        RocksDbVecState {
            common,
            _phantom: Default::default(),
        }
    }
}

impl<IK, N, T, F> ReducingStateBuilder<IK, N, T, F> for RocksDb
where
    IK: Serialize,
    N: Serialize,
    T: Serialize + for<'a> Deserialize<'a>,
    F: Fn(&T, &T) -> T + Send + Sync + Clone + 'static,
{
    type Type = RocksDbReducingState<IK, N, T, F>;

    fn new_reducing_state(
        &mut self,
        name: &str,
        init_item_key: IK,
        init_namespace: N,
        reduce_fn: F,
    ) -> Self::Type {
        let common = StateCommon::new_for_reducing_state(
            self,
            name,
            init_item_key,
            init_namespace,
            reduce_fn.clone(),
        );
        RocksDbReducingState {
            common,
            reduce_fn,
            _phantom: Default::default(),
        }
    }
}

impl<IK, N, T, AGG> AggregatingStateBuilder<IK, N, T, AGG> for RocksDb
where
    IK: Serialize,
    N: Serialize,
    T: Serialize + for<'a> Deserialize<'a>,
    AGG: Aggregator<T> + Send + Sync + Clone + 'static,
    AGG::Accumulator: Serialize + for<'a> Deserialize<'a>,
{
    type Type = RocksDbAggregatingState<IK, N, T, AGG>;

    fn new_aggregating_state(
        &mut self,
        name: &str,
        init_item_key: IK,
        init_namespace: N,
        aggregator: AGG,
    ) -> Self::Type {
        let common = StateCommon::new_for_aggregating_state(
            self,
            name,
            init_item_key,
            init_namespace,
            aggregator.clone(),
        );
        RocksDbAggregatingState {
            common,
            aggregator,
            _phantom: Default::default(),
        }
    }
}

mod state_common {
    use super::*;

    pub(crate) struct StateCommon<IK, N> {
        pub cf_name: String,
        pub item_key: IK,
        pub namespace: N,
    }

    fn common_options<IK: Serialize, N: Serialize>(item_key: &IK, namespace: &N) -> Options {
        // The line below should yield the same size for any values of given types IK, N. This is
        // not enforced anywhere yet, but we rely on it. For example, neither IK nor N should be
        // Vec<T> or String, because those types serialize to variable length byte arrays.
        // TODO: restrict possible IK and N with a trait? We could add an associated const there,
        //  so the computation below is eliminated.
        let prefix_size = bincode::serialized_size(&(item_key, namespace))
            .expect("Couldn't compute prefix size for column family"); // TODO: propagate

        // base opts
        let mut opts = Options::default();
        // for map state to work properly, but useful for all the states, so the bloom filters get
        // populated
        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(prefix_size as usize));

        opts
    }

    impl<IK, N> StateCommon<IK, N>
    where
        IK: Serialize,
        N: Serialize,
    {
        pub fn get_db_key<UK>(&self, user_key: &UK) -> ArconResult<Vec<u8>>
        where
            UK: Serialize,
        {
            let res = bincode::serialize(&(&self.item_key, &self.namespace, user_key))
                .map_err(|e| arcon_err_kind!("Could not serialize keys and namespace: {}", e))?;

            Ok(res)
        }

        fn new_for_basic_state(
            backend: &mut RocksDb,
            name: &str,
            item_key: IK,
            namespace: N,
        ) -> StateCommon<IK, N> {
            let opts = common_options(&item_key, &namespace);
            let cf_name = backend.get_or_create_column_family(name, opts);

            StateCommon {
                cf_name,
                item_key,
                namespace,
            }
        }

        pub fn new_for_value_state(
            backend: &mut RocksDb,
            name: &str,
            item_key: IK,
            namespace: N,
        ) -> StateCommon<IK, N> {
            let full_name = format!("value_{}", name);
            Self::new_for_basic_state(backend, &full_name, item_key, namespace)
        }

        pub fn new_for_map_state(
            backend: &mut RocksDb,
            name: &str,
            item_key: IK,
            namespace: N,
        ) -> StateCommon<IK, N> {
            let full_name = format!("map_{}", name);
            Self::new_for_basic_state(backend, &full_name, item_key, namespace)
        }

        pub fn new_for_vec_state(
            backend: &mut RocksDb,
            name: &str,
            item_key: IK,
            namespace: N,
        ) -> StateCommon<IK, N> {
            let mut opts = common_options(&item_key, &namespace);

            opts.set_merge_operator_associative("vec_merge", vec_state::vec_merge);

            let full_name = format!("vec_{}", name);
            let cf_name = backend.get_or_create_column_family(&full_name, opts);
            StateCommon {
                cf_name,
                item_key,
                namespace,
            }
        }

        pub fn new_for_reducing_state<T, F>(
            backend: &mut RocksDb,
            name: &str,
            item_key: IK,
            namespace: N,
            reduce_fn: F,
        ) -> StateCommon<IK, N>
        where
            T: Serialize + for<'a> Deserialize<'a>,
            F: Fn(&T, &T) -> T + Sync + Send + Clone + 'static,
        {
            let mut opts = common_options(&item_key, &namespace);

            let reducing_merge = reducing_state::make_reducing_merge(reduce_fn);
            opts.set_merge_operator_associative("reducing_merge", reducing_merge);

            let full_name = format!("reducing_{}", name);
            let cf_name = backend.get_or_create_column_family(&full_name, opts);
            StateCommon {
                cf_name,
                item_key,
                namespace,
            }
        }

        pub fn new_for_aggregating_state<T, AGG>(
            backend: &mut RocksDb,
            name: &str,
            item_key: IK,
            namespace: N,
            aggregator: AGG,
        ) -> StateCommon<IK, N>
        where
            T: for<'a> Deserialize<'a>,
            AGG: Aggregator<T> + Send + Sync + Clone + 'static,
            AGG::Accumulator: Serialize + for<'a> Deserialize<'a>,
        {
            let mut opts = common_options(&item_key, &namespace);

            let aggregate_merge = aggregating_state::make_aggregating_merge(aggregator);
            opts.set_merge_operator_associative("aggregate_merge", aggregate_merge);

            let full_name = format!("aggregating_{}", name);
            let cf_name = backend.get_or_create_column_family(&full_name, opts);
            StateCommon {
                cf_name,
                item_key,
                namespace,
            }
        }
    }
}

mod aggregating_state;
mod map_state;
mod reducing_state;
mod value_state;
mod vec_state;

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::{Deref, DerefMut};
    use tempfile::TempDir;

    pub struct TestDb {
        rocks: RocksDb,
        _dir: TempDir,
    }

    impl TestDb {
        pub fn new() -> TestDb {
            let dir = TempDir::new().unwrap();
            let dir_path = dir.path().to_string_lossy().into_owned();
            let rocks = RocksDb::new(&dir_path).unwrap();
            TestDb { rocks, _dir: dir }
        }
    }

    impl Deref for TestDb {
        type Target = RocksDb;

        fn deref(&self) -> &Self::Target {
            &self.rocks
        }
    }

    impl DerefMut for TestDb {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.rocks
        }
    }

    #[test]
    fn test_unit_state_key_empty() {
        let state = StateCommon {
            cf_name: "".to_string(),
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

        db.put(column_family, key.as_bytes(), value.as_bytes())
            .expect("put");

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
        let mut checkpoints_dir_path = checkpoints_dir.path().to_path_buf();
        checkpoints_dir_path.push("chkp0");
        let checkpoints_dir_path = checkpoints_dir_path.to_string_lossy();
        let checkpoints_dir_path = checkpoints_dir_path.as_ref();

        let mut db = RocksDb::new(&dir_path).unwrap();

        let key: &[u8] = b"key";
        let initial_value: &[u8] = b"value";
        let new_value: &[u8] = b"new value";
        let column_family = "default";

        db.put(column_family, key, initial_value)
            .expect("put failed");
        db.checkpoint(checkpoints_dir_path.into())
            .expect("checkpoint failed");
        db.put(column_family, key, new_value)
            .expect("second put failed");

        let db_from_checkpoint =
            RocksDb::new(&checkpoints_dir_path).expect("Could not open checkpointed db");

        assert_eq!(
            new_value,
            db.get(column_family, key)
                .expect("Could not get from the original db")
                .as_ref()
        );
        assert_eq!(
            initial_value,
            db_from_checkpoint
                .get(column_family, key)
                .expect("Could not get from the checkpoint")
                .as_ref()
        );
    }
}
