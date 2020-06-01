// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
use crate::{
    error::*, Aggregator, AggregatorState, Backend, BackendContainer, Handle, Key, MapState,
    Metakey, Reducer, ReducerState, Value, ValueState, VecState,
};
use custom_debug::CustomDebug;
use rocksdb::{
    checkpoint::Checkpoint, ColumnFamily, ColumnFamilyDescriptor, DBPinnableSlice, Options,
    SliceTransform, WriteBatch, WriteOptions, DB,
};
use std::{
    collections::{HashMap, HashSet},
    fs,
    iter::FromIterator,
    mem,
    path::{Path, PathBuf},
};

#[derive(Debug)]
pub struct Rocks {
    inner: Inner,
    path: PathBuf,
    restored: bool,
}

#[derive(CustomDebug)]
enum Inner {
    Initialized(#[debug(skip)] InitializedRocksDb),
    Uninitialized {
        unknown_cfs: HashSet<String>,
        #[debug(skip)]
        known_cfs: HashMap<String, Options>,
    },
}

struct InitializedRocksDb {
    db: DB,
    // this is here so we can easily clear the column family by dropping and recreating it
    options: HashMap<String, Options>,
}

// we use epochs, so WAL is useless for us
fn default_write_opts() -> WriteOptions {
    let mut res = WriteOptions::default();
    res.disable_wal(true);
    res
}

impl InitializedRocksDb {
    fn get_cf_handle(&self, cf_name: impl AsRef<str>) -> Result<&ColumnFamily> {
        let cf_name = cf_name.as_ref();
        self.db
            .cf_handle(cf_name)
            .with_context(|| RocksMissingColumnFamily {
                cf_name: cf_name.to_string(),
            })
    }

    fn get(
        &self,
        cf_name: impl AsRef<str>,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<DBPinnableSlice>> {
        let cf = self.get_cf_handle(cf_name)?;
        Ok(self.db.get_pinned_cf(cf, key)?)
    }

    fn put(
        &mut self,
        cf_name: impl AsRef<str>,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<()> {
        let cf = self.get_cf_handle(cf_name)?;
        Ok(self.db.put_cf_opt(cf, key, value, &default_write_opts())?)
    }

    fn remove(&mut self, cf: impl AsRef<str>, key: impl AsRef<[u8]>) -> Result<()> {
        let cf = self.get_cf_handle(cf)?;
        Ok(self.db.delete_cf_opt(cf, key, &default_write_opts())?)
    }

    fn remove_prefix(&mut self, cf: impl AsRef<str>, prefix: impl AsRef<[u8]>) -> Result<()> {
        let prefix = prefix.as_ref();
        let cf_name = cf.as_ref();

        if prefix.is_empty() {
            // prefix is empty, so we use the fast path of dropping and re-creating the whole
            // column family

            let cf_opts = &self
                .options
                .get(cf_name)
                .with_context(|| RocksMissingOptions {
                    cf_name: cf_name.to_string(),
                })?;

            self.db.drop_cf(cf_name)?;
            self.db.create_cf(cf_name, cf_opts)?;
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
        wb.delete_range_cf(cf, start, &end)?;

        self.db.write_opt(wb, &default_write_opts())?;

        Ok(())
    }

    fn contains(&self, cf: impl AsRef<str>, key: impl AsRef<[u8]>) -> Result<bool> {
        let cf = self.get_cf_handle(cf.as_ref())?;
        Ok(self.db.get_pinned_cf(cf, key)?.is_some())
    }

    fn create_column_family_if_doesnt_exist(&mut self, cf_name: &str, opts: Options) -> Result<()> {
        if self.db.cf_handle(cf_name).is_none() {
            self.db.create_cf(cf_name, &opts)?;
            self.options.insert(cf_name.into(), opts);
        }

        Ok(())
    }
}

impl Rocks {
    #[inline]
    fn initialized(&self) -> Result<&InitializedRocksDb> {
        match &self.inner {
            Inner::Initialized(i) => Ok(i),
            Inner::Uninitialized { unknown_cfs, .. } => RocksUninitialized {
                unknown_cfs: unknown_cfs.clone(),
            }
            .fail(),
        }
    }

    #[inline]
    fn initialized_mut(&mut self) -> Result<&mut InitializedRocksDb> {
        match &mut self.inner {
            Inner::Initialized(i) => Ok(i),
            Inner::Uninitialized { unknown_cfs, .. } => RocksUninitialized {
                unknown_cfs: unknown_cfs.clone(),
            }
            .fail(),
        }
    }

    #[inline]
    fn get(
        &self,
        cf_name: impl AsRef<str>,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<DBPinnableSlice>> {
        self.initialized()?.get(cf_name, key)
    }

    #[inline]
    fn put(
        &mut self,
        cf_name: impl AsRef<str>,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<()> {
        self.initialized_mut()?.put(cf_name, key, value)
    }

    #[inline]
    fn remove(&mut self, cf_name: impl AsRef<str>, key: impl AsRef<[u8]>) -> Result<()> {
        self.initialized_mut()?.remove(cf_name, key)
    }

    #[inline]
    fn remove_prefix(&mut self, cf: impl AsRef<str>, prefix: impl AsRef<[u8]>) -> Result<()> {
        self.initialized_mut()?.remove_prefix(cf, prefix)
    }

    #[inline]
    fn contains(&self, cf: impl AsRef<str>, key: impl AsRef<[u8]>) -> Result<bool> {
        self.initialized()?.contains(cf, key)
    }

    fn create_column_family(&mut self, cf_name: &str, opts: Options) -> Result<()> {
        match &mut self.inner {
            Inner::Initialized(i) => i.create_column_family_if_doesnt_exist(cf_name, opts)?,
            Inner::Uninitialized {
                unknown_cfs,
                known_cfs,
            } => {
                if known_cfs.contains_key(cf_name) {
                    return Ok(());
                }

                unknown_cfs.remove(cf_name);
                known_cfs.insert(cf_name.to_string(), opts);

                if unknown_cfs.is_empty() {
                    self.initialize()?;
                }
            }
        }

        Ok(())
    }

    fn initialize(&mut self) -> Result<()> {
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
            db: DB::open_cf_descriptors_borrowed(&whole_db_opts, &self.path, &cfds)?,
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

fn common_options<IK, N>() -> Options
where
    IK: Metakey,
    N: Metakey,
{
    let prefix_size = IK::SIZE + N::SIZE;

    let mut opts = Options::default();
    // for map state to work properly, but useful for all the states, so the bloom filters get
    // populated
    opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(prefix_size as usize));

    opts
}

impl Backend for Rocks {
    fn create(path: &Path) -> Result<BackendContainer<Self>>
    where
        Self: Sized,
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);

        let path: PathBuf = path.into();
        if !path.exists() {
            fs::create_dir_all(&path)?;
        }
        let path = path
            .canonicalize()
            .ok()
            .with_context(|| InvalidPath { path: path.clone() })?;

        let column_families: HashSet<String> = match DB::list_cf(&opts, &path) {
            Ok(cfs) => cfs.into_iter().filter(|n| n != "default").collect(),
            // TODO: possibly platform-dependant error message check
            Err(e) if e.to_string().contains("No such file or directory") => HashSet::new(),
            Err(e) => Err(e)?,
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
                db: DB::open_cf_descriptors_borrowed(&opts, &path, &cfds)?,
                options: cfds
                    .into_iter()
                    .map(|cfd| (cfd.name, cfd.options))
                    .collect(),
            })
        };

        Ok(BackendContainer::new(Rocks {
            inner,
            path,
            restored: false,
        }))
    }

    fn restore(live_path: &Path, checkpoint_path: &Path) -> Result<BackendContainer<Self>>
    where
        Self: Sized,
    {
        fs::create_dir_all(live_path)?;

        ensure!(
            fs::read_dir(live_path)?.next().is_none(),
            RocksRestoreDirNotEmpty {
                dir: live_path.clone()
            }
        );

        let mut target_path: PathBuf = live_path.into();
        target_path.push("__DUMMY"); // the file name is replaced inside the loop below
        for entry in fs::read_dir(checkpoint_path)? {
            let entry = entry?;

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

            fs::copy(&source_path, &target_path)?;
        }

        Rocks::create(live_path).map(|mut r| {
            r.get_mut().restored = true;
            r
        })
    }

    fn was_restored(&self) -> bool {
        self.restored
    }

    fn checkpoint(&self, checkpoint_path: &Path) -> Result<()> {
        let InitializedRocksDb { db, .. } = self.initialized()?;

        db.flush()?;

        let checkpointer = Checkpoint::new(db)?;

        if checkpoint_path.exists() {
            // TODO: add a warning log here
            // warn!(logger, "Checkpoint path {:?} exists, deleting");
            fs::remove_dir_all(checkpoint_path)?
        }

        checkpointer.create_checkpoint(checkpoint_path)?;
        Ok(())
    }

    fn register_value_handle<'s, T: Value, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<ValueState<T>, IK, N>,
    ) {
        handle.registered = true;
        let opts = common_options::<IK, N>();
        self.create_column_family(&handle.id, opts)
            .expect("Could not create column family");
    }

    fn register_map_handle<'s, K: Key, V: Value, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<MapState<K, V>, IK, N>,
    ) {
        handle.registered = true;
        let opts = common_options::<IK, N>();
        self.create_column_family(&handle.id, opts)
            .expect("Could not create column family");
    }

    fn register_vec_handle<'s, T: Value, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<VecState<T>, IK, N>,
    ) {
        handle.registered = true;
        let mut opts = common_options::<IK, N>();
        opts.set_merge_operator_associative("vec_merge", vec_ops::vec_merge);
        self.create_column_family(&handle.id, opts)
            .expect("Could not create column family");
    }

    fn register_reducer_handle<'s, T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<ReducerState<T, F>, IK, N>,
    ) {
        handle.registered = true;
        let mut opts = common_options::<IK, N>();
        let reducer_merge = reducer_ops::make_reducer_merge(handle.extra_data.clone());
        opts.set_merge_operator_associative("reducer_merge", reducer_merge);
        self.create_column_family(&handle.id, opts)
            .expect("Could not create column family");
    }

    fn register_aggregator_handle<'s, A: Aggregator, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<AggregatorState<A>, IK, N>,
    ) {
        handle.registered = true;
        let mut opts = common_options::<IK, N>();
        let aggregator_merge = aggregator_ops::make_aggregator_merge(handle.extra_data.clone());
        opts.set_merge_operator_associative("aggregator_merge", aggregator_merge);
        self.create_column_family(&handle.id, opts)
            .expect("Could not create column family");
    }
}

mod aggregator_ops;
mod map_ops;
mod reducer_ops;
mod value_ops;
mod vec_ops;

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::RegistrationToken;
    use std::ops::{Deref, DerefMut};
    use tempfile::TempDir;

    #[derive(Debug)]
    pub struct TestDb {
        rocks: BackendContainer<Rocks>,
        dir: TempDir,
    }

    impl TestDb {
        pub fn new() -> TestDb {
            let dir = TempDir::new().unwrap();
            let mut dir_path = dir.path().to_path_buf();
            dir_path.push("rocks");
            fs::create_dir(&dir_path).unwrap();
            let rocks = Rocks::create(&dir_path).unwrap();
            TestDb { rocks, dir }
        }

        pub fn checkpoint(&mut self) -> PathBuf {
            let mut checkpoint_dir: PathBuf = self.dir.path().into();
            checkpoint_dir.push("checkpoint");
            self.rocks.get_mut().checkpoint(&checkpoint_dir).unwrap();
            checkpoint_dir
        }

        pub fn from_checkpoint(checkpoint_dir: &str) -> TestDb {
            let dir = TempDir::new().unwrap();
            let mut dir_path = dir.path().to_path_buf();
            dir_path.push("rocks");
            let rocks = Rocks::restore(&dir_path, checkpoint_dir.as_ref()).unwrap();
            TestDb { rocks, dir }
        }
    }

    impl Deref for TestDb {
        type Target = BackendContainer<Rocks>;

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
    fn simple_rocksdb_test() {
        let mut db = TestDb::new();

        let key = "key";
        let value = "test";
        let column_family = "default";

        db.get_mut()
            .put(column_family, key.as_bytes(), value.as_bytes())
            .expect("put");

        {
            let v = db
                .get_mut()
                .get(column_family, key.as_bytes())
                .unwrap()
                .unwrap();
            assert_eq!(value, String::from_utf8_lossy(&v));
        }

        db.get_mut()
            .remove(column_family, key.as_bytes())
            .expect("remove");
        let v = db.get_mut().get(column_family, key.as_bytes()).unwrap();
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

        let mut db = Rocks::create(&dir_path).unwrap();

        let key: &[u8] = b"key";
        let initial_value: &[u8] = b"value";
        let new_value: &[u8] = b"new value";
        let column_family = "default";

        db.get_mut()
            .put(column_family, key, initial_value)
            .expect("put failed");
        db.get_mut()
            .checkpoint(&checkpoints_dir_path)
            .expect("checkpoint failed");
        db.get_mut()
            .put(column_family, key, new_value)
            .expect("second put failed");

        let mut db_from_checkpoint = Rocks::restore(&restore_dir_path, &checkpoints_dir_path)
            .expect("Could not open checkpointed db");

        assert_eq!(
            new_value,
            db.get_mut()
                .get(column_family, key)
                .expect("Could not get from the original db")
                .unwrap()
                .as_ref()
        );
        assert_eq!(
            initial_value,
            db_from_checkpoint
                .get_mut()
                .get(column_family, key)
                .expect("Could not get from the checkpoint")
                .unwrap()
                .as_ref()
        );
    }

    #[test]
    fn checkpoint_restore_state_test() {
        let mut original_test = TestDb::new();
        let mut original = original_test.session();
        let mut a_handle = Handle::value("a");
        a_handle.register(&mut unsafe { RegistrationToken::new(&mut original) });

        let checkpoint_dir = {
            let mut a = a_handle.activate(&mut original);

            a.set(420).unwrap();

            drop(original);
            let checkpoint_dir = original_test.checkpoint();

            // re-session, because checkpointing requires a mutable borrow
            let mut original = original_test.session();
            let mut a = a_handle.activate(&mut original);

            assert_eq!(a.get().unwrap().unwrap(), 420);
            a.set(69).unwrap();
            assert_eq!(a.get().unwrap().unwrap(), 69);

            checkpoint_dir
        };

        let restored = TestDb::from_checkpoint(&checkpoint_dir.to_string_lossy());
        let mut restored = restored.session();
        a_handle.register(&mut unsafe { RegistrationToken::new(&mut restored) });
        {
            let mut a_restored = a_handle.activate(&mut restored);
            // TODO: serialize value state metadata (type names, serialization, etc.) into rocksdb, so
            //   that type mismatches are caught early. Right now it would be possible to, let's say,
            //   store an integer, and then read a float from the restored state backend
            assert_eq!(a_restored.get().unwrap().unwrap(), 420);

            a_restored.set(1337).unwrap();
            assert_eq!(a_restored.get().unwrap().unwrap(), 1337);

            let mut original = original_test.session();
            let a = a_handle.activate(&mut original);
            assert_eq!(a.get().unwrap().unwrap(), 69);
        }
    }

    #[allow(irrefutable_let_patterns)]
    #[test]
    fn missing_state_raises_errors() {
        let mut original_test = TestDb::new();
        let mut original = original_test.session();

        let mut a_handle = Handle::value("a");
        a_handle.register(&mut unsafe { RegistrationToken::new(&mut original) });
        let mut b_handle = Handle::value("b");
        b_handle.register(&mut unsafe { RegistrationToken::new(&mut original) });

        let mut a = a_handle.activate(&mut original);
        a.set(420).unwrap();
        let mut b = b_handle.activate(&mut original);
        b.set(69).unwrap();

        drop(original);
        let checkpoint_dir = original_test.checkpoint();

        let restored = TestDb::from_checkpoint(&checkpoint_dir.to_string_lossy());
        let mut restored = restored.session();
        // original backend had two states created, and here we try to mess with state before we
        // declare all the states
        a_handle.register(&mut unsafe { RegistrationToken::new(&mut restored) });
        let a = a_handle.activate(&mut restored);
        if let ArconStateError::RocksUninitialized { unknown_cfs, .. } = a.get().unwrap_err() {
            assert_eq!(unknown_cfs.into_iter().collect::<Vec<_>>(), vec!["b"])
        } else {
            panic!("Error should have been returned")
        }
    }

    common_state_tests!(TestDb::new());
}
