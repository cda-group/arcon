// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{Key, Metakey, Value},
    error::*,
    Aggregator, AggregatorState, Backend, Handle, MapState, Reducer, ReducerState, ValueState,
    VecState,
};
use rocksdb::{
    checkpoint::Checkpoint, ColumnFamily, ColumnFamilyDescriptor, DBPinnableSlice, Options,
    SliceTransform, WriteBatch, WriteOptions, DB,
};
use std::{
    cell::UnsafeCell,
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};

unsafe impl Send for Rocks {}
unsafe impl Sync for Rocks {}

#[derive(Debug)]
pub struct Rocks {
    inner: UnsafeCell<DB>,
    path: PathBuf,
    restored: bool,
}

// we use epochs, so WAL is useless for us
fn default_write_opts() -> WriteOptions {
    let mut res = WriteOptions::default();
    res.disable_wal(true);
    res
}

impl Rocks {
    #[inline(always)]
    #[allow(clippy::mut_from_ref)]
    fn db_mut(&self) -> &mut DB {
        unsafe { &mut (*self.inner.get()) }
    }

    #[inline(always)]
    fn db(&self) -> &DB {
        unsafe { &(*self.inner.get()) }
    }

    #[inline]
    fn get_cf_handle(&self, cf_name: impl AsRef<str>) -> Result<&ColumnFamily> {
        let cf_name = cf_name.as_ref();
        self.db()
            .cf_handle(cf_name)
            .with_context(|| RocksMissingColumnFamily {
                cf_name: cf_name.to_string(),
            })
    }

    #[inline]
    fn get(
        &self,
        cf_name: impl AsRef<str>,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<DBPinnableSlice>> {
        let cf = self.get_cf_handle(cf_name)?;
        Ok(self.db().get_pinned_cf(cf, key)?)
    }

    #[inline]
    fn put(
        &self,
        cf_name: impl AsRef<str>,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<()> {
        let cf = self.get_cf_handle(cf_name)?;
        Ok(self
            .db()
            .put_cf_opt(cf, key, value, &default_write_opts())?)
    }

    #[inline]
    fn remove(&self, cf: impl AsRef<str>, key: impl AsRef<[u8]>) -> Result<()> {
        let cf = self.get_cf_handle(cf)?;
        Ok(self.db().delete_cf_opt(cf, key, &default_write_opts())?)
    }

    fn remove_prefix(&self, cf: impl AsRef<str>, prefix: impl AsRef<[u8]>) -> Result<()> {
        let prefix = prefix.as_ref();
        let cf_name = cf.as_ref();

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
        wb.delete_range_cf(cf, start, &end);

        self.db().write_opt(wb, &default_write_opts())?;

        Ok(())
    }

    #[inline]
    fn contains(&self, cf: impl AsRef<str>, key: impl AsRef<[u8]>) -> Result<bool> {
        let cf = self.get_cf_handle(cf.as_ref())?;
        Ok(self.db().get_pinned_cf(cf, key)?.is_some())
    }

    fn create_column_family(&self, cf_name: &str, opts: Options) -> Result<()> {
        if self.db().cf_handle(cf_name).is_none() {
            self.db_mut().create_cf(cf_name, &opts)?;
        }
        Ok(())
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
    fn create(path: &Path) -> Result<Self>
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
            Err(e) => return Err(e.into()),
        };

        let cfds = if !column_families.is_empty() {
            column_families
                .into_iter()
                .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
                .collect()
        } else {
            vec![ColumnFamilyDescriptor::new("default", Options::default())]
        };

        Ok(Rocks {
            inner: UnsafeCell::new(DB::open_cf_descriptors(&opts, &path, cfds)?),
            path,
            restored: false,
        })
    }

    fn restore(live_path: &Path, checkpoint_path: &Path) -> Result<Self>
    where
        Self: Sized,
    {
        fs::create_dir_all(live_path)?;

        ensure!(
            fs::read_dir(live_path)?.next().is_none(),
            RocksRestoreDirNotEmpty { dir: &(*live_path) }
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
            //r.get_mut().restored = true;
            r.restored = true;
            r
        })
    }

    fn was_restored(&self) -> bool {
        self.restored
    }

    fn checkpoint(&self, checkpoint_path: &Path) -> Result<()> {
        let db = self.db();
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
        &'s self,
        handle: &'s mut Handle<ValueState<T>, IK, N>,
    ) {
        handle.registered = true;
        let opts = common_options::<IK, N>();
        self.create_column_family(&handle.id, opts)
            .expect("Could not create column family");
    }

    fn register_map_handle<'s, K: Key, V: Value, IK: Metakey, N: Metakey>(
        &'s self,
        handle: &'s mut Handle<MapState<K, V>, IK, N>,
    ) {
        handle.registered = true;
        let opts = common_options::<IK, N>();
        self.create_column_family(&handle.id, opts)
            .expect("Could not create column family");
    }

    fn register_vec_handle<'s, T: Value, IK: Metakey, N: Metakey>(
        &'s self,
        handle: &'s mut Handle<VecState<T>, IK, N>,
    ) {
        handle.registered = true;
        let mut opts = common_options::<IK, N>();
        opts.set_merge_operator_associative("vec_merge", vec_ops::vec_merge);
        self.create_column_family(&handle.id, opts)
            .expect("Could not create column family");
    }

    fn register_reducer_handle<'s, T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &'s self,
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
        &'s self,
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
    use std::{
        ops::{Deref, DerefMut},
        sync::Arc,
    };
    use tempfile::TempDir;

    #[derive(Debug)]
    pub struct TestDb {
        rocks: Arc<Rocks>,
        dir: TempDir,
    }

    impl TestDb {
        #[allow(clippy::new_without_default)]
        pub fn new() -> TestDb {
            let dir = TempDir::new().unwrap();
            let mut dir_path = dir.path().to_path_buf();
            dir_path.push("rocks");
            fs::create_dir(&dir_path).unwrap();
            let rocks = Rocks::create(&dir_path).unwrap();
            TestDb {
                rocks: Arc::new(rocks),
                dir,
            }
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
            let rocks = Rocks::restore(&dir_path, checkpoint_dir.as_ref()).unwrap();
            TestDb {
                rocks: Arc::new(rocks),
                dir,
            }
        }
    }

    impl Deref for TestDb {
        type Target = Arc<Rocks>;

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
        let db = TestDb::new();

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

        let db = Rocks::create(&dir_path).unwrap();

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

        let db_from_checkpoint = Rocks::restore(&restore_dir_path, &checkpoints_dir_path)
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
        let mut original_test = TestDb::new();
        let mut a_handle = Handle::value("a");
        original_test.register_value_handle(&mut a_handle);

        let checkpoint_dir = {
            let mut a = a_handle.activate(original_test.clone());

            a.set(420).unwrap();

            let checkpoint_dir = original_test.checkpoint();
            assert_eq!(a.get().unwrap().unwrap(), 420);
            a.set(69).unwrap();
            assert_eq!(a.get().unwrap().unwrap(), 69);
            checkpoint_dir
        };

        let restored = TestDb::from_checkpoint(&checkpoint_dir.to_string_lossy());

        {
            let mut a_handle = Handle::value("a");
            restored.register_value_handle(&mut a_handle);
            let mut a_restored = a_handle.activate(restored.clone());
            // TODO: serialize value state metadata (type names, serialization, etc.) into rocksdb, so
            //   that type mismatches are caught early. Right now it would be possible to, let's say,
            //   store an integer, and then read a float from the restored state backend
            assert_eq!(a_restored.get().unwrap().unwrap(), 420);

            a_restored.set(1337).unwrap();
            assert_eq!(a_restored.get().unwrap().unwrap(), 1337);
        }
    }

    common_state_tests!(TestDb::new());
}
