// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::state_backend::{
    builders::*,
    rocks::{
        aggregating_state::RocksDbAggregatingState, map_state::RocksDbMapState,
        reducing_state::RocksDbReducingState, state_common::StateCommon,
        value_state::RocksDbValueState, vec_state::RocksDbVecState,
    },
    serialization::{DeserializableWith, SerializableFixedSizeWith, SerializableWith},
    state_types::*,
    StateBackend,
};
use arcon_error::*;
use rocksdb::{
    checkpoint::Checkpoint, ColumnFamily, ColumnFamilyDescriptor, DBPinnableSlice, Options,
    SliceTransform, WriteBatch, DB,
};
use std::{
    collections::{HashMap, HashSet},
    fs,
    iter::FromIterator,
    mem,
    path::{Path, PathBuf},
};

pub struct RocksDb {
    inner: Inner,
    path: PathBuf,
    restored: bool,
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

    fn get(
        &self,
        cf_name: impl AsRef<str>,
        key: impl AsRef<[u8]>,
    ) -> ArconResult<Option<DBPinnableSlice>> {
        let cf = self.get_cf_handle(cf_name)?;

        self.db
            .get_pinned_cf(cf, key)
            .map_err(|e| arcon_err_kind!("Could not get map state value: {}", e))
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
        let prefix = prefix.as_ref();
        let cf_name = cf.as_ref();

        if prefix.is_empty() {
            // prefix is empty, so we use the fast path of dropping and re-creating the whole
            // column family

            let cf_opts = &self.options.get(cf_name).ok_or_else(|| {
                arcon_err_kind!("Missing options for column family '{}'", cf_name)
            })?;

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

    fn create_column_family_if_doesnt_exist(
        &mut self,
        cf_name: &str,
        opts: Options,
    ) -> ArconResult<()> {
        if self.db.cf_handle(cf_name).is_none() {
            self.db
                .create_cf(cf_name, &opts)
                .map_err(|e| arcon_err_kind!("Could not create column family: {}", e))?;
            self.options.insert(cf_name.into(), opts);
        }

        Ok(())
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
    fn get(
        &self,
        cf_name: impl AsRef<str>,
        key: impl AsRef<[u8]>,
    ) -> ArconResult<Option<DBPinnableSlice>> {
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

    fn get_or_create_column_family(&mut self, cf_name: &str, opts: Options) -> ArconResult<String> {
        match &mut self.inner {
            Inner::Initialized(i) => i.create_column_family_if_doesnt_exist(cf_name, opts)?,
            Inner::Uninitialized {
                unknown_cfs,
                known_cfs,
            } => {
                if known_cfs.contains_key(cf_name) {
                    return Ok(cf_name.to_string());
                }

                unknown_cfs.remove(cf_name);
                known_cfs.insert(cf_name.to_string(), opts);

                if unknown_cfs.is_empty() {
                    self.initialize()?;
                }
            }
        }

        Ok(cf_name.to_string())
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
    fn new(path: &Path) -> ArconResult<RocksDb> {
        let mut opts = Options::default();
        opts.create_if_missing(true);

        let path: PathBuf = path.into();
        let path = path
            .canonicalize()
            .map_err(|e| arcon_err_kind!("Cannot canonicalize path {:?}: {}", path, e))?;

        let column_families: HashSet<String> = match DB::list_cf(&opts, &path) {
            Ok(cfs) => cfs.into_iter().filter(|n| n != "default").collect(),
            // TODO: possibly platform-dependant error message check
            Err(e) if e.to_string().contains("No such file or directory") => HashSet::new(),
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

        Ok(RocksDb {
            inner,
            path,
            restored: false,
        })
    }

    fn checkpoint(&self, checkpoint_path: &Path) -> ArconResult<()> {
        let InitializedRocksDb { db, .. } = self.initialized()?;

        db.flush()
            .map_err(|e| arcon_err_kind!("Could not flush rocksdb: {}", e))?;

        let checkpointer = Checkpoint::new(db)
            .map_err(|e| arcon_err_kind!("Could not create checkpoint object: {}", e))?;

        if checkpoint_path.exists() {
            // TODO: add a warning log here
            // warn!(logger, "Checkpoint path {:?} exists, deleting");
            fs::remove_dir_all(checkpoint_path)
                .ctx("Could not remove existing checkpoint directory")?
        }

        checkpointer
            .create_checkpoint(checkpoint_path)
            .map_err(|e| arcon_err_kind!("Could not save the checkpoint: {}", e))?;
        Ok(())
    }

    fn restore(restore_path: &Path, checkpoint_path: &Path) -> ArconResult<Self>
    where
        Self: Sized,
    {
        fs::create_dir_all(restore_path)
            .map_err(|e| arcon_err_kind!("Could not create restore directory: {}", e))?;

        if fs::read_dir(restore_path)
            .map_err(|e| arcon_err_kind!("Could not read directory contents: {}", e))?
            .next()
            .is_some()
        {
            return arcon_err!("Restore path '{:?}' is not empty!", restore_path);
        }

        let mut target_path: PathBuf = restore_path.into();
        target_path.push("__DUMMY"); // the file name is replaced inside the loop below
        for entry in fs::read_dir(checkpoint_path).map_err(|e| {
            arcon_err_kind!(
                "Could not read checkpoint directory '{:?}': {}",
                checkpoint_path,
                e
            )
        })? {
            let entry = entry.map_err(|e| {
                arcon_err_kind!("Could not inspect checkpoint directory entry: {}", e)
            })?;

            assert!(entry
                .file_type()
                .expect("Cannot read entry metadata")
                .is_file());

            let source_path = entry.path();
            // replaces the __DUMMY from above the loop
            target_path.set_file_name(
                source_path
                    .file_name()
                    .expect("directory entry with no name?"),
            );

            fs::copy(&source_path, &target_path).map_err(|e| {
                arcon_err_kind!(
                    "Could not copy rocks file '{}': {}",
                    source_path.to_string_lossy(),
                    e
                )
            })?;
        }

        RocksDb::new(restore_path).map(|mut r| {
            r.restored = true;
            r
        })
    }

    fn was_restored(&self) -> bool {
        self.restored
    }
}

impl<IK, N, T, KS, TS> ValueStateBuilder<IK, N, T, KS, TS> for RocksDb
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
{
    type Type = RocksDbValueState<IK, N, T, KS, TS>;

    fn new_value_state(
        &mut self,
        name: &str,
        item_key: IK,
        namespace: N,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type {
        let common = StateCommon::new_for_value_state(
            self,
            name,
            item_key,
            namespace,
            key_serializer,
            value_serializer,
        );
        RocksDbValueState {
            common,
            _phantom: Default::default(),
        }
    }
}

impl<IK, N, K, V, KS, TS> MapStateBuilder<IK, N, K, V, KS, TS> for RocksDb
where
    IK: SerializableFixedSizeWith<KS> + DeserializableWith<KS>,
    N: SerializableFixedSizeWith<KS> + DeserializableWith<KS>,
    K: SerializableWith<KS> + DeserializableWith<KS>,
    V: SerializableWith<TS> + DeserializableWith<TS>,
    KS: Clone + 'static,
    TS: Clone + 'static,
{
    type Type = RocksDbMapState<IK, N, K, V, KS, TS>;

    fn new_map_state(
        &mut self,
        name: &str,
        item_key: IK,
        namespace: N,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type {
        let common = StateCommon::new_for_map_state(
            self,
            name,
            item_key,
            namespace,
            key_serializer,
            value_serializer,
        );
        RocksDbMapState {
            common,
            _phantom: Default::default(),
        }
    }
}

impl<IK, N, T, KS, TS> VecStateBuilder<IK, N, T, KS, TS> for RocksDb
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
{
    type Type = RocksDbVecState<IK, N, T, KS, TS>;

    fn new_vec_state(
        &mut self,
        name: &str,
        item_key: IK,
        namespace: N,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type {
        let common = StateCommon::new_for_vec_state(
            self,
            name,
            item_key,
            namespace,
            key_serializer,
            value_serializer,
        );
        RocksDbVecState {
            common,
            _phantom: Default::default(),
        }
    }
}

impl<IK, N, T, F, KS, TS> ReducingStateBuilder<IK, N, T, F, KS, TS> for RocksDb
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
    F: Fn(&T, &T) -> T + Send + Sync + Clone + 'static,
    TS: Send + Sync + Clone + 'static,
{
    type Type = RocksDbReducingState<IK, N, T, F, KS, TS>;

    fn new_reducing_state(
        &mut self,
        name: &str,
        item_key: IK,
        namespace: N,
        reduce_fn: F,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type {
        let common = StateCommon::new_for_reducing_state(
            self,
            name,
            item_key,
            namespace,
            reduce_fn.clone(),
            key_serializer,
            value_serializer,
        );
        RocksDbReducingState {
            common,
            reduce_fn,
            _phantom: Default::default(),
        }
    }
}

impl<IK, N, T, AGG, KS, TS> AggregatingStateBuilder<IK, N, T, AGG, KS, TS> for RocksDb
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
    AGG: Aggregator<T> + Send + Sync + Clone + 'static,
    AGG::Accumulator: SerializableWith<TS> + DeserializableWith<TS>,
    TS: Send + Sync + Clone + 'static,
{
    type Type = RocksDbAggregatingState<IK, N, T, AGG, KS, TS>;

    fn new_aggregating_state(
        &mut self,
        name: &str,
        item_key: IK,
        namespace: N,
        aggregator: AGG,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type {
        let common = StateCommon::new_for_aggregating_state(
            self,
            name,
            item_key,
            namespace,
            aggregator.clone(),
            key_serializer,
            value_serializer,
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

    pub(crate) struct StateCommon<IK, N, KS, TS> {
        pub cf_name: String,
        pub item_key: IK,
        pub namespace: N,
        pub key_serializer: KS,
        pub value_serializer: TS,
    }

    fn common_options<IK, N, KS>(_item_key: &IK, _namespace: &N, _key_serializer: &KS) -> Options
    where
        IK: SerializableFixedSizeWith<KS>,
        N: SerializableFixedSizeWith<KS>,
    {
        let prefix_size = IK::SIZE + N::SIZE;

        let mut opts = Options::default();
        // for map state to work properly, but useful for all the states, so the bloom filters get
        // populated
        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(prefix_size as usize));

        opts
    }

    impl<IK, N, KS, TS> StateCommon<IK, N, KS, TS>
    where
        IK: SerializableFixedSizeWith<KS>,
        N: SerializableFixedSizeWith<KS>,
    {
        pub fn get_db_key_with_user_key<UK>(&self, user_key: &UK) -> ArconResult<Vec<u8>>
        where
            UK: SerializableWith<KS>,
        {
            let mut res = Vec::with_capacity(
                IK::SIZE + N::SIZE + UK::size_hint(&self.key_serializer, user_key).unwrap_or(0),
            );
            IK::serialize_into(&self.key_serializer, &mut res, &self.item_key)?;
            N::serialize_into(&self.key_serializer, &mut res, &self.namespace)?;
            UK::serialize_into(&self.key_serializer, &mut res, user_key)?;

            Ok(res)
        }

        pub fn get_db_key_prefix(&self) -> ArconResult<Vec<u8>> {
            let mut res = Vec::with_capacity(IK::SIZE + N::SIZE);
            IK::serialize_into(&self.key_serializer, &mut res, &self.item_key)?;
            N::serialize_into(&self.key_serializer, &mut res, &self.namespace)?;

            Ok(res)
        }

        fn new_for_basic_state(
            backend: &mut RocksDb,
            name: &str,
            item_key: IK,
            namespace: N,
            key_serializer: KS,
            value_serializer: TS,
        ) -> StateCommon<IK, N, KS, TS> {
            let opts = common_options(&item_key, &namespace, &key_serializer);
            let cf_name = backend
                .get_or_create_column_family(name, opts)
                .expect("Could not create column family");

            StateCommon {
                cf_name,
                item_key,
                namespace,
                key_serializer,
                value_serializer,
            }
        }

        pub fn new_for_value_state(
            backend: &mut RocksDb,
            name: &str,
            item_key: IK,
            namespace: N,
            key_serializer: KS,
            value_serializer: TS,
        ) -> StateCommon<IK, N, KS, TS> {
            let full_name = format!("value_{}", name);
            Self::new_for_basic_state(
                backend,
                &full_name,
                item_key,
                namespace,
                key_serializer,
                value_serializer,
            )
        }

        pub fn new_for_map_state(
            backend: &mut RocksDb,
            name: &str,
            item_key: IK,
            namespace: N,
            key_serializer: KS,
            value_serializer: TS,
        ) -> StateCommon<IK, N, KS, TS> {
            let full_name = format!("map_{}", name);
            Self::new_for_basic_state(
                backend,
                &full_name,
                item_key,
                namespace,
                key_serializer,
                value_serializer,
            )
        }

        pub fn new_for_vec_state(
            backend: &mut RocksDb,
            name: &str,
            item_key: IK,
            namespace: N,
            key_serializer: KS,
            value_serializer: TS,
        ) -> StateCommon<IK, N, KS, TS> {
            let mut opts = common_options(&item_key, &namespace, &key_serializer);

            opts.set_merge_operator_associative("vec_merge", vec_state::vec_merge);

            let full_name = format!("vec_{}", name);
            let cf_name = backend
                .get_or_create_column_family(&full_name, opts)
                .expect("Could not create column family");
            StateCommon {
                cf_name,
                item_key,
                namespace,
                key_serializer,
                value_serializer,
            }
        }

        pub fn new_for_reducing_state<T, F>(
            backend: &mut RocksDb,
            name: &str,
            item_key: IK,
            namespace: N,
            reduce_fn: F,
            key_serializer: KS,
            value_serializer: TS,
        ) -> StateCommon<IK, N, KS, TS>
        where
            T: SerializableWith<TS> + DeserializableWith<TS>,
            F: Fn(&T, &T) -> T + Send + Sync + Clone + 'static,
            TS: Send + Sync + Clone + 'static,
        {
            let mut opts = common_options(&item_key, &namespace, &key_serializer);

            let reducing_merge =
                reducing_state::make_reducing_merge(reduce_fn, value_serializer.clone());
            opts.set_merge_operator_associative("reducing_merge", reducing_merge);

            let full_name = format!("reducing_{}", name);
            let cf_name = backend
                .get_or_create_column_family(&full_name, opts)
                .expect("Could not create column family");
            StateCommon {
                cf_name,
                item_key,
                namespace,
                key_serializer,
                value_serializer,
            }
        }

        pub fn new_for_aggregating_state<T, AGG>(
            backend: &mut RocksDb,
            name: &str,
            item_key: IK,
            namespace: N,
            aggregator: AGG,
            key_serializer: KS,
            value_serializer: TS,
        ) -> StateCommon<IK, N, KS, TS>
        where
            T: DeserializableWith<TS>,
            AGG: Aggregator<T> + Send + Sync + Clone + 'static,
            AGG::Accumulator: SerializableWith<TS> + DeserializableWith<TS>,
            TS: Send + Sync + Clone + 'static,
        {
            let mut opts = common_options(&item_key, &namespace, &key_serializer);

            let aggregate_merge =
                aggregating_state::make_aggregating_merge(aggregator, value_serializer.clone());
            opts.set_merge_operator_associative("aggregate_merge", aggregate_merge);

            let full_name = format!("aggregating_{}", name);
            let cf_name = backend
                .get_or_create_column_family(&full_name, opts)
                .expect("Could not create column family");
            StateCommon {
                cf_name,
                item_key,
                namespace,
                key_serializer,
                value_serializer,
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
pub mod test {
    use super::*;
    use crate::state_backend::serialization::NativeEndianBytesDump;
    use std::ops::{Deref, DerefMut};
    use tempfile::TempDir;

    pub struct TestDb {
        rocks: RocksDb,
        dir: TempDir,
    }

    impl TestDb {
        pub fn new() -> TestDb {
            let dir = TempDir::new().unwrap();
            let mut dir_path = dir.path().to_path_buf();
            dir_path.push("rocks");
            fs::create_dir(&dir_path).unwrap();
            let rocks = RocksDb::new(&dir_path).unwrap();
            TestDb { rocks, dir }
        }

        pub fn checkpoint(&mut self) -> PathBuf {
            let mut checkpoint_dir: PathBuf = self.dir.path().into();
            checkpoint_dir.push("checkpoint");
            self.rocks.checkpoint(&checkpoint_dir).unwrap();
            checkpoint_dir
        }

        pub fn from_checkpoint(checkpoint_dir: &str) -> TestDb {
            let dir = TempDir::new().unwrap();
            let mut dir_path = dir.path().to_path_buf();
            dir_path.push("rocks");
            let rocks = RocksDb::restore(&dir_path, checkpoint_dir.as_ref()).unwrap();
            TestDb { rocks, dir }
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
            key_serializer: NativeEndianBytesDump,
            value_serializer: NativeEndianBytesDump,
        };

        let v = state.get_db_key_prefix().unwrap();

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
            let v = db.get(column_family, key.as_bytes()).unwrap().unwrap();
            assert_eq!(value, String::from_utf8_lossy(&v));
        }

        db.remove(column_family, key.as_bytes()).expect("remove");
        let v = db.get(column_family, key.as_bytes()).unwrap();
        assert!(v.is_none());
    }

    #[test]
    fn checkpoint_rocksdb_raw_test() {
        let tmp_dir = TempDir::new().unwrap();
        let checkpoints_dir = TempDir::new().unwrap();
        let restore_dir = TempDir::new().unwrap();

        let dir_path = tmp_dir.path();

        let mut checkpoints_dir_path = checkpoints_dir.path().to_path_buf();
        checkpoints_dir_path.push("chkp0");

        let mut restore_dir_path = restore_dir.path().to_path_buf();
        restore_dir_path.push("chkp0");

        let mut db = RocksDb::new(&dir_path).unwrap();

        let key: &[u8] = b"key";
        let initial_value: &[u8] = b"value";
        let new_value: &[u8] = b"new value";
        let column_family = "default";

        db.put(column_family, key, initial_value)
            .expect("put failed");
        db.checkpoint(&checkpoints_dir_path)
            .expect("checkpoint failed");
        db.put(column_family, key, new_value)
            .expect("second put failed");

        let db_from_checkpoint = RocksDb::restore(&restore_dir_path, &checkpoints_dir_path)
            .expect("Could not open checkpointed db");

        assert_eq!(
            new_value,
            db.get(column_family, key)
                .expect("Could not get from the original db")
                .unwrap()
                .as_ref()
        );
        assert_eq!(
            initial_value,
            db_from_checkpoint
                .get(column_family, key)
                .expect("Could not get from the checkpoint")
                .unwrap()
                .as_ref()
        );
    }

    #[test]
    fn checkpoint_restore_state_test() {
        let mut original = TestDb::new();
        let a_value =
            original.new_value_state("a", (), (), NativeEndianBytesDump, NativeEndianBytesDump);
        a_value.set(&mut original, 420).unwrap();

        let checkpoint_dir = original.checkpoint();

        assert_eq!(a_value.get(&original).unwrap().unwrap(), 420);
        a_value.set(&mut original, 69).unwrap();
        assert_eq!(a_value.get(&original).unwrap().unwrap(), 69);

        let mut restored = TestDb::from_checkpoint(&checkpoint_dir.to_string_lossy());
        // TODO: serialize value state metadata (type names, serialization, etc.) into rocksdb, so
        //   that type mismatches are caught early. Right now it would be possible to, let's say,
        //   store an integer, and then read a float from the restored state backend
        let a_value_restored =
            restored.new_value_state("a", (), (), NativeEndianBytesDump, NativeEndianBytesDump);
        assert_eq!(a_value_restored.get(&restored).unwrap().unwrap(), 420);

        a_value_restored.set(&mut restored, 1337).unwrap();
        assert_eq!(a_value_restored.get(&restored).unwrap().unwrap(), 1337);
        assert_eq!(a_value.get(&original).unwrap().unwrap(), 69);
    }

    #[allow(irrefutable_let_patterns)]
    #[test]
    fn missing_state_raises_errors() {
        let mut original = TestDb::new();
        let a_value =
            original.new_value_state("a", (), (), NativeEndianBytesDump, NativeEndianBytesDump);
        let b_value =
            original.new_value_state("b", (), (), NativeEndianBytesDump, NativeEndianBytesDump);
        a_value.set(&mut original, 420).unwrap();
        b_value.set(&mut original, 69).unwrap();

        let checkpoint_dir = original.checkpoint();

        let mut restored = TestDb::from_checkpoint(&checkpoint_dir.to_string_lossy());
        let a_value_restored: RocksDbValueState<_, _, i32, _, _> =
            restored.new_value_state("a", (), (), NativeEndianBytesDump, NativeEndianBytesDump);
        // original backend had two states created, and here we try to mess with state before we
        // declare all the states
        if let ErrorKind::ArconError(message) = a_value_restored.get(&restored).unwrap_err().kind()
        {
            assert_eq!(
                &*message,
                "Database not initialized, missing cf descriptors: {\"value_b\"}" // TODO: hardcoded error message :(
            )
        } else {
            panic!("Error should have been returned")
        }
    }

    #[test]
    fn test_key_serialization() {
        let mut db = TestDb::new();
        let state = StateCommon::new_for_map_state(
            &mut db,
            "test-name",
            0u8,
            0u8,
            NativeEndianBytesDump,
            NativeEndianBytesDump,
        );
        let key = state
            .get_db_key_with_user_key(&"foobar".to_string())
            .unwrap();
        assert_eq!(key.len(), 1 + 1 + std::mem::size_of::<usize>() + 6);
    }
}
