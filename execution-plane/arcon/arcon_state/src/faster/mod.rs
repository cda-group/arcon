// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
use crate::{
    error::*, Aggregator, AggregatorState, Backend, BackendContainer, Handle, Key, MapState,
    Metakey, Reducer, ReducerState, Value, ValueState, VecState,
};
use custom_debug::CustomDebug;
use faster_rs::{status, FasterKv, FasterKvBuilder, FasterRmw};
use serde::{Deserialize, Serialize};
use std::{
    cell::Cell,
    collections::HashMap,
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(CustomDebug)]
pub struct Faster {
    #[debug(skip)]
    db: FasterKv,
    monotonic_serial_number: Cell<u64>,
    #[debug(skip)]
    aggregate_fns: HashMap<&'static str, Box<dyn Fn(&[u8], &[u8]) -> Vec<u8> + Send>>,
    dir: PathBuf,
    restored: bool,
}

fn copy_checkpoint(
    token: &str,
    db_directory: impl Into<PathBuf> + Copy,
    checkpoint_path: impl Into<PathBuf> + Copy,
) -> Result<()> {
    use std::fs;

    // create the checkpoint folders
    let mut checkpoint_cpr: PathBuf = checkpoint_path.into();
    checkpoint_cpr.push("cpr-checkpoints");
    checkpoint_cpr.push(token);
    fs::create_dir_all(&checkpoint_cpr)?;

    let mut checkpoint_index: PathBuf = checkpoint_path.into();
    checkpoint_index.push("index-checkpoints");
    checkpoint_index.push(token);
    fs::create_dir_all(&checkpoint_index)?;

    let mut source_dir: PathBuf = db_directory.into();
    let mut checkpoint_root: PathBuf = checkpoint_path.into();

    let copy_files =
        |source_dir: &Path, dest_dir: &mut PathBuf, silently_ignore_dirs: bool| -> Result<()> {
            for file in fs::read_dir(&source_dir)? {
                let file = file?;
                let file_name = file.file_name();
                let path = file.path();

                if silently_ignore_dirs && file.file_type()?.is_dir() {
                    continue;
                }

                dest_dir.push(file_name);
                fs::copy(path, &dest_dir)?;
                dest_dir.pop();
            }
            Ok(())
        };

    // copy over the log files
    copy_files(&source_dir, &mut checkpoint_root, true)?;

    // copy over the cpr checkpoint files
    source_dir.push("cpr-checkpoints");
    source_dir.push(token);
    copy_files(&source_dir, &mut checkpoint_cpr, false)?;
    source_dir.pop();
    source_dir.pop();

    // copy over the index files
    source_dir.push("index-checkpoints");
    source_dir.push(token);
    copy_files(&source_dir, &mut checkpoint_index, false)?;

    Ok(())
}

// NOTE: weird types (&Vec<u8>, which should be &[u8]) are due to the design of the faster-rs lib
impl Faster {
    #[inline(always)]
    fn next_serial_number(&self) -> u64 {
        let res = self.monotonic_serial_number.get();
        self.monotonic_serial_number.set(res + 1);
        res
    }

    fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>> {
        // TODO: make the return of `read` more rusty
        let (status, receiver) = self.db.read(key, self.next_serial_number());
        match status {
            status::NOT_FOUND => Ok(None),
            status::OK | status::PENDING => {
                let val = receiver
                    .recv_timeout(Duration::from_secs(2)) // TODO: make that customizable
                    .context(FasterReceiveTimeout)?;
                Ok(Some(val))
            }
            _ => FasterUnexpectedStatus { status }.fail(),
        }
    }

    fn get_vec(&self, key: &Vec<u8>) -> Result<Option<Vec<Vec<u8>>>> {
        let (status, receiver) = self.db.read(key, self.next_serial_number());
        match status {
            status::NOT_FOUND => Ok(None),
            status::OK | status::PENDING => {
                let vec_ops: FasterVecOps = receiver
                    .recv_timeout(Duration::from_secs(2)) // TODO: make that customizable
                    .context(FasterReceiveTimeout)?;

                use FasterVecOps::*;
                let val = match vec_ops {
                    Value(v) => v,
                    Push(single) | PushIfAbsent(single) => vec![single],
                    _ => panic!("invalid faster vec ops value"), // this is always a bug
                };

                Ok(Some(val))
            }
            _ => FasterUnexpectedStatus { status }.fail(),
        }
    }

    fn put(&mut self, key: &Vec<u8>, value: &Vec<u8>) -> Result<()> {
        // TODO: make the return of `upsert` more rusty
        let status = self.db.upsert(key, value, self.next_serial_number());
        match status {
            status::OK | status::PENDING => Ok(()),
            _ => FasterUnexpectedStatus { status }.fail(),
        }
    }

    fn remove(&mut self, key: &Vec<u8>) -> Result<()> {
        let status = self.db.delete(key, self.next_serial_number());
        match status {
            status::OK | status::PENDING | status::NOT_FOUND => Ok(()),
            _ => FasterUnexpectedStatus { status }.fail(),
        }
    }

    fn vec_remove(&mut self, key: &Vec<u8>, to_remove: Vec<u8>) -> Result<()> {
        let status = self.db.rmw(
            key,
            &FasterVecOps::Remove(to_remove),
            self.next_serial_number(),
        );

        match status {
            status::OK | status::PENDING => Ok(()),
            _ => FasterUnexpectedStatus { status }.fail(),
        }
    }

    fn vec_push(&mut self, key: &Vec<u8>, to_push: Vec<u8>) -> Result<()> {
        let status = self
            .db
            .rmw(key, &FasterVecOps::Push(to_push), self.next_serial_number());

        match status {
            status::OK | status::PENDING => Ok(()),
            _ => FasterUnexpectedStatus { status }.fail(),
        }
    }

    fn vec_push_all(&mut self, key: &Vec<u8>, to_push: Vec<Vec<u8>>) -> Result<()> {
        let status = self.db.rmw(
            key,
            &FasterVecOps::Value(to_push),
            self.next_serial_number(),
        );

        match status {
            status::OK | status::PENDING => Ok(()),
            _ => FasterUnexpectedStatus { status }.fail(),
        }
    }

    fn vec_push_if_absent(&mut self, key: &Vec<u8>, to_push: Vec<u8>) -> Result<()> {
        let status = self.db.rmw(
            key,
            &FasterVecOps::PushIfAbsent(to_push),
            self.next_serial_number(),
        );

        match status {
            status::OK | status::PENDING => Ok(()),
            _ => FasterUnexpectedStatus { status }.fail(),
        }
    }

    fn vec_set(&mut self, key: &Vec<u8>, value: Vec<Vec<u8>>) -> Result<()> {
        let status = self
            .db
            .upsert(key, &FasterVecOps::Value(value), self.next_serial_number());
        match status {
            status::OK | status::PENDING => Ok(()),
            _ => FasterUnexpectedStatus { status }.fail(),
        }
    }

    fn aggregate(&mut self, key: &Vec<u8>, new: Vec<u8>, aggregate_id: &str) -> Result<()> {
        let fun = self
            .aggregate_fns
            .get(aggregate_id)
            .expect("aggregate fn missing; handle not registered?")
            .as_ref();
        let fun_fat_ptr_bytes = unsafe { std::mem::transmute(fun) };

        let status = self.db.rmw(
            key,
            &FasterAgg::Modify(new, fun_fat_ptr_bytes),
            self.next_serial_number(),
        );

        match status {
            status::OK | status::PENDING => Ok(()),
            _ => FasterUnexpectedStatus { status }.fail(),
        }
    }

    fn get_agg(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>> {
        let (status, receiver) = self.db.read(key, self.next_serial_number());
        match status {
            status::NOT_FOUND => Ok(None),
            status::OK | status::PENDING => {
                let vec_ops: FasterAgg = receiver
                    .recv_timeout(Duration::from_secs(2)) // TODO: make that customizable
                    .context(FasterReceiveTimeout)?;

                let val = match vec_ops {
                    FasterAgg::Value(v) => v,
                    FasterAgg::Modify(v, _fn_fat_ptr_bytes) => v,
                };

                Ok(Some(val))
            }
            _ => FasterUnexpectedStatus { status }.fail(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum FasterVecOps {
    Value(Vec<Vec<u8>>),
    Push(Vec<u8>),
    PushIfAbsent(Vec<u8>),
    Remove(Vec<u8>),
    RemoveIdx(usize),
}

impl FasterRmw for FasterVecOps {
    fn rmw(&self, modification: Self) -> Self {
        use FasterVecOps::*;
        let mut res = match self {
            Value(v) => v.clone(),
            Push(single) | PushIfAbsent(single) => vec![single.clone()],
            _ => panic!("invalid faster vec ops value"),
        };

        match modification {
            Value(elems) => res.extend_from_slice(&elems),
            Push(elem) => res.push(elem),
            PushIfAbsent(elem) => {
                if !res.contains(&elem) {
                    res.push(elem);
                }
            }
            Remove(elem) => {
                let idx = res.iter().position(|i| i == &elem);
                if let Some(idx) = idx {
                    res.remove(idx);
                }
                // does nothing if the item doesn't exist
            }
            RemoveIdx(idx) => {
                res.remove(idx);
            }
        }

        Value(res)
    }
}

// HACK: we box the closure and serialize a raw pointer to it
#[derive(Serialize, Deserialize)]
enum FasterAgg {
    Value(Vec<u8>),
    Modify(
        Vec<u8>,
        [u8; std::mem::size_of::<&dyn Fn(&[u8], &[u8]) -> Vec<u8>>()],
    ),
}

impl FasterRmw for FasterAgg {
    fn rmw(&self, modification: Self) -> Self {
        let old = match self {
            FasterAgg::Value(v) => v,
            FasterAgg::Modify(v, _fun_fat_ptr_bytes) => v,
        };

        if let FasterAgg::Modify(new, fun_fat_ptr_bytes) = modification {
            let f: &dyn Fn(&[u8], &[u8]) -> Vec<u8> =
                unsafe { std::mem::transmute(fun_fat_ptr_bytes) };
            FasterAgg::Value(f(old, &new))
        } else {
            panic!("modification argument must be Agg::Modify");
        }
    }
}

impl Backend for Faster {
    fn create(live_path: &Path) -> Result<BackendContainer<Self>>
    where
        Self: Sized,
    {
        // TODO: figure the params out
        let db = FasterKvBuilder::new(1 << 15, 1024 * 1024 * 1024)
            .with_disk(live_path.to_str().with_context(|| InvalidPath {
                path: live_path.to_path_buf(),
            })?)
            .build()?;

        Ok(BackendContainer::new(Faster {
            db,
            monotonic_serial_number: Cell::new(0),
            aggregate_fns: HashMap::new(),
            dir: live_path.into(),
            restored: false,
        }))
    }

    fn restore(live_path: &Path, checkpoint_path: &Path) -> Result<BackendContainer<Self>>
    where
        Self: Sized,
    {
        let mut chkp_info_path: PathBuf = checkpoint_path.into();
        chkp_info_path.push("ARCON_CHECKPOINT_INFO");

        let mut token = String::with_capacity(36);
        let num_read = File::open(&chkp_info_path)?.read_to_string(&mut token)?;

        assert_eq!(num_read, 36);

        copy_checkpoint(&token, checkpoint_path, live_path)?;

        let mut res = Faster::create(live_path)?;
        let restore_result = res.get_mut().db.recover(token.clone(), token)?;

        match restore_result.status {
            status::OK => (),
            status::PENDING => res.get_mut().db.complete_pending(true),
            _ => {
                return FasterUnexpectedStatus {
                    status: restore_result.status,
                }
                .fail()
            }
        }

        res.get_mut().restored = true;

        Ok(res)
    }

    fn was_restored(&self) -> bool {
        self.restored
    }

    fn checkpoint(&self, checkpoint_path: &Path) -> Result<()> {
        // start completing pending operations and wait till they are done (the `true` param)
        self.db.complete_pending(true);

        let chkp = self.db.checkpoint()?;

        if !chkp.checked {
            return FasterCheckpointFailed.fail();
        }

        self.db.complete_pending(true);

        copy_checkpoint(&chkp.token, &self.dir, checkpoint_path)?;

        let mut chkp_info_path: PathBuf = checkpoint_path.into();
        chkp_info_path.push("ARCON_CHECKPOINT_INFO");
        File::create(&chkp_info_path)?.write_all(chkp.token.as_bytes())?;

        Ok(())
    }

    fn start_session(&mut self) {
        self.db.start_session();
    }

    fn session_drop_hook(&mut self) -> Option<Box<dyn FnOnce(&mut Self)>> {
        Some(Box::new(|this| this.db.stop_session()))
    }

    fn register_value_handle<'s, T: Value, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<ValueState<T>, IK, N>,
    ) {
        handle.registered = true;
    }

    fn register_map_handle<'s, K: Key, V: Value, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<MapState<K, V>, IK, N>,
    ) {
        handle.registered = true;
    }

    fn register_vec_handle<'s, T: Value, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<VecState<T>, IK, N>,
    ) {
        handle.registered = true;
    }

    fn register_reducer_handle<'s, T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<ReducerState<T, F>, IK, N>,
    ) {
        self.aggregate_fns.insert(
            handle.id,
            reducer_ops::make_reduce_fn(handle.extra_data.clone()),
        );
        handle.registered = true;
    }

    fn register_aggregator_handle<'s, A: Aggregator, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<AggregatorState<A>, IK, N>,
    ) {
        self.aggregate_fns.insert(
            handle.id,
            aggregator_ops::make_aggregate_fn(handle.extra_data.clone()),
        );
        handle.registered = true;
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
        fs,
        ops::{Deref, DerefMut},
    };
    use tempfile::TempDir;

    #[derive(Debug)]
    pub struct TestDb {
        faster: BackendContainer<Faster>,
        dir: TempDir,
    }

    impl TestDb {
        pub fn new() -> TestDb {
            let dir = TempDir::new().unwrap();
            let mut dir_path = dir.path().to_path_buf();
            dir_path.push("faster");
            fs::create_dir(&dir_path).unwrap();
            let faster = Faster::create(&dir_path).unwrap();
            TestDb { faster, dir }
        }

        pub fn checkpoint(&mut self) -> PathBuf {
            let mut checkpoint_dir = self.dir.path().to_path_buf();
            checkpoint_dir.push("checkpoint");
            self.faster.get_mut().checkpoint(&checkpoint_dir).unwrap();
            checkpoint_dir
        }

        pub fn from_checkpoint(checkpoint_dir: &str) -> TestDb {
            let dir = TempDir::new().unwrap();
            let mut dir_path = dir.path().to_path_buf();
            dir_path.push("faster");
            let faster = Faster::restore(checkpoint_dir.as_ref(), &dir_path).unwrap();
            TestDb { faster, dir }
        }
    }

    impl Deref for TestDb {
        type Target = BackendContainer<Faster>;

        fn deref(&self) -> &Self::Target {
            &self.faster
        }
    }

    impl DerefMut for TestDb {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.faster
        }
    }

    #[test]
    fn test_faster_checkpoints() {
        let dir = tempfile::TempDir::new().unwrap();
        let dir = dir.path().to_string_lossy().into_owned();
        let mut faster = Faster::create(dir.as_ref()).unwrap();
        let mut faster_session = faster.session();

        faster_session
            .backend
            .put(&b"a".to_vec(), &b"1".to_vec())
            .unwrap();
        faster_session
            .backend
            .put(&b"b".to_vec(), &b"2".to_vec())
            .unwrap();

        let one = faster_session.backend.get(&b"a".to_vec()).unwrap().unwrap();
        let two = faster_session.backend.get(&b"b".to_vec()).unwrap().unwrap();

        assert_eq!(&one, b"1");
        assert_eq!(&two, b"2");

        let chkp_dir = tempfile::TempDir::new().unwrap();
        let restore_dir = tempfile::TempDir::new().unwrap();

        drop(faster_session);
        faster.get_mut().checkpoint(chkp_dir.path()).unwrap();

        let mut restored = Faster::restore(restore_dir.path(), chkp_dir.path()).unwrap();

        assert!(!faster.get_mut().was_restored());
        assert!(restored.get_mut().was_restored());

        let mut restored_session = restored.session();

        let one = restored_session
            .backend
            .get(&b"a".to_vec())
            .unwrap()
            .unwrap();
        let two = restored_session
            .backend
            .get(&b"b".to_vec())
            .unwrap()
            .unwrap();

        assert_eq!(&one, b"1");
        assert_eq!(&two, b"2");

        restored_session.backend.remove(&b"a".to_vec()).unwrap();
        restored_session
            .backend
            .put(&b"c".to_vec(), &b"3".to_vec())
            .unwrap();

        let chkp2_dir = tempfile::TempDir::new().unwrap();
        let restore2_dir = tempfile::TempDir::new().unwrap();

        drop(restored_session);
        restored.get_mut().checkpoint(chkp2_dir.path()).unwrap();

        let restored2 = Faster::restore(restore2_dir.path(), chkp2_dir.path()).unwrap();
        let restored2_session = restored2.session();

        let one = restored2_session.backend.get(&b"a".to_vec()).unwrap();
        let two = restored2_session
            .backend
            .get(&b"b".to_vec())
            .unwrap()
            .unwrap();
        let three = restored2_session
            .backend
            .get(&b"c".to_vec())
            .unwrap()
            .unwrap();

        assert_eq!(one, None);
        assert_eq!(&two, b"2");
        assert_eq!(&three, b"3");
    }

    common_state_tests!(TestDb::new());
}
