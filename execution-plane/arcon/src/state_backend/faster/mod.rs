// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::{
        AggregatingStateBuilder, ArconResult, ReducingStateBuilder, ValueStateBuilder,
        VecStateBuilder,
    },
    state_backend::{
        faster::{
            aggregating_state::FasterAggregatingState, map_state::FasterMapState,
            reducing_state::FasterReducingState, value_state::FasterValueState,
            vec_state::FasterVecState,
        },
        serialization::{DeserializableWith, LittleEndianBytesDump, SerializableWith},
        state_types::Aggregator,
        MapStateBuilder, StateBackend,
    },
};
use error::ResultExt;
use faster_rs::{status, FasterKv, FasterKvBuilder, FasterRmw};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

pub struct Faster {
    db: FasterKv,
    monotonic_serial_number: AtomicU64,
    dir: PathBuf,
    restored: bool,
}

impl StateBackend for Faster {
    fn new(path: &Path) -> ArconResult<Self>
    where
        Self: Sized,
    {
        // TODO: figure the params out
        let db = FasterKvBuilder::new(1 << 15, 1024 * 1024 * 1024)
            .with_disk(
                path.to_str()
                    .ok_or_else(|| arcon_err_kind!("Invalid path"))?,
            )
            .build()
            .ctx("Could not open faster")?;

        Ok(Faster {
            db,
            monotonic_serial_number: AtomicU64::new(0),
            dir: path.into(),
            restored: false,
        })
    }

    fn checkpoint(&self, checkpoint_path: &Path) -> ArconResult<()> {
        // start completing pending operations and wait till they are done (the `true` param)
        self.db.complete_pending(true);

        let chkp = self
            .db
            .checkpoint()
            .map_err(|e| arcon_err_kind!("Could not checkpoint: {}", e))?;
        if !chkp.checked {
            return arcon_err!("Checkpoint failed");
        }

        self.db.complete_pending(true);

        copy_checkpoint(&chkp.token, &self.dir, checkpoint_path)?;

        let mut chkp_info_path: PathBuf = checkpoint_path.into();
        chkp_info_path.push("ARCON_CHECKPOINT_INFO");
        File::create(&chkp_info_path)
            .ctx("Could not create checkpoint info file")?
            .write_all(chkp.token.as_bytes())
            .ctx("Could not write the checkpoint token")?;

        Ok(())
    }

    fn restore(restore_path: &Path, checkpoint_path: &Path) -> ArconResult<Self>
    where
        Self: Sized,
    {
        let mut chkp_info_path: PathBuf = checkpoint_path.into();
        chkp_info_path.push("ARCON_CHECKPOINT_INFO");

        let mut token = String::with_capacity(36);
        let num_read = File::open(&chkp_info_path)
            .ctx("Could not open the checkpoint info file")?
            .read_to_string(&mut token)
            .ctx("Could not read the checkpoint token")?;

        assert_eq!(num_read, 36);

        copy_checkpoint(&token, checkpoint_path, restore_path)?;

        let mut res = Faster::new(restore_path)?;
        let restore_result = res
            .db
            .recover(token.clone(), token)
            .map_err(|e| arcon_err_kind!("Restore error: {}", e))?;

        match restore_result.status {
            status::OK => (),
            status::PENDING => res.db.complete_pending(true),
            _ => return arcon_err!("Restore failure: {}", restore_result.status),
        }

        res.restored = true;

        Ok(res)
    }

    fn was_restored(&self) -> bool {
        self.restored
    }
}

fn copy_checkpoint(
    token: &str,
    db_directory: impl Into<PathBuf> + Copy,
    checkpoint_path: impl Into<PathBuf> + Copy,
) -> ArconResult<()> {
    use std::fs;

    // create the checkpoint folders
    let mut checkpoint_cpr: PathBuf = checkpoint_path.into();
    checkpoint_cpr.push("cpr-checkpoints");
    checkpoint_cpr.push(token);
    fs::create_dir_all(&checkpoint_cpr).ctx("Could not create checkpoint directories")?;

    let mut checkpoint_index: PathBuf = checkpoint_path.into();
    checkpoint_index.push("index-checkpoints");
    checkpoint_index.push(token);
    fs::create_dir_all(&checkpoint_index).ctx("Could not create checkpoint directories")?;

    let mut db_dir_path_buf: PathBuf = db_directory.into();
    let mut checkpoint_path_buf: PathBuf = checkpoint_path.into();
    // copy over the log files
    for file in fs::read_dir(&db_dir_path_buf).ctx("Could not read faster db dir")? {
        let file = file.ctx("Could not read file metadata")?;

        if file
            .file_type()
            .ctx("Could not determine file type")?
            .is_dir()
        {
            continue;
        }

        checkpoint_path_buf.push(file.file_name());
        fs::copy(file.path(), &checkpoint_path_buf).ctx("Could not copy file")?;
        checkpoint_path_buf.pop();
    }

    // copy over the cpr checkpoint files
    db_dir_path_buf.push("cpr-checkpoints");
    db_dir_path_buf.push(token);
    for file in
        fs::read_dir(&db_dir_path_buf).ctx("Could not read faster cpr checkpoint directory")?
    {
        let file = file.ctx("Could not read file metadata")?;
        let file_name = file.file_name();
        let path = file.path();

        checkpoint_cpr.push(file_name);
        fs::copy(path, &checkpoint_cpr).ctx("Could not copy the cpr files")?;
        checkpoint_cpr.pop();
    }

    // copy over the index files
    db_dir_path_buf.pop();
    db_dir_path_buf.pop();
    db_dir_path_buf.push("index-checkpoints");
    db_dir_path_buf.push(token);
    for file in
        fs::read_dir(&db_dir_path_buf).ctx("Could not read faster index checkpoint directory")?
    {
        let file = file.ctx("Could not read file metadata")?;
        let file_name = file.file_name();
        let path = file.path();

        checkpoint_index.push(file_name);
        fs::copy(path, &checkpoint_index).ctx("Could not copy the cpr files")?;
        checkpoint_index.pop();
    }

    Ok(())
}

// NOTE: weird types (&Vec<u8>, which should be &[u8]) are due to the design of the faster-rs lib
impl Faster {
    fn next_serial_number(&self) -> u64 {
        self.monotonic_serial_number.fetch_add(1, Ordering::SeqCst)
    }

    pub fn in_session<T>(&self, f: impl FnOnce(&Self) -> T) -> T {
        self.db.start_session();
        let res = f(self);
        self.db.stop_session();
        res
    }

    pub fn in_session_mut<T>(&mut self, f: impl FnOnce(&mut Self) -> T) -> T {
        self.db.start_session();
        let res = f(self);
        self.db.stop_session();
        res
    }

    fn get(&self, key: &Vec<u8>) -> ArconResult<Option<Vec<u8>>> {
        // TODO: make the return of `read` more rusty
        let (status, receiver) = self.db.read(key, self.next_serial_number());
        match status {
            status::NOT_FOUND => Ok(None),
            status::OK | status::PENDING => {
                let val = receiver
                    .recv_timeout(Duration::from_secs(2)) // TODO: make that customizable
                    .ctx("Could not receive result from faster thread")?;
                Ok(Some(val))
            }
            _ => arcon_err!("faster get error: {}", status),
        }
    }

    fn get_vec(&self, key: &Vec<u8>) -> ArconResult<Option<Vec<Vec<u8>>>> {
        let (status, receiver) = self.db.read(key, self.next_serial_number());
        match status {
            status::NOT_FOUND => Ok(None),
            status::OK | status::PENDING => {
                let vec_ops: FasterVecOps = receiver
                    .recv_timeout(Duration::from_secs(2)) // TODO: make that customizable
                    .ctx("Could not receive result from faster thread")?;

                use FasterVecOps::*;
                let val = match vec_ops {
                    Value(v) => v,
                    Push(single) | PushIfAbsent(single) => vec![single],
                    _ => return arcon_err!("invalid faster vec ops value"),
                };

                Ok(Some(val))
            }
            _ => arcon_err!("faster get error: {}", status),
        }
    }

    fn put(&mut self, key: &Vec<u8>, value: &Vec<u8>) -> ArconResult<()> {
        // TODO: make the return of `upsert` more rusty
        let status = self.db.upsert(key, value, self.next_serial_number());
        match status {
            status::OK | status::PENDING => Ok(()),
            _ => arcon_err!("faster put error: {}", status),
        }
    }

    fn remove(&mut self, key: &Vec<u8>) -> ArconResult<()> {
        let status = self.db.delete(key, self.next_serial_number());
        match status {
            status::OK | status::PENDING | status::NOT_FOUND => Ok(()),
            _ => arcon_err!("faster remove error: {}", status),
        }
    }

    fn vec_remove(&mut self, key: &Vec<u8>, to_remove: Vec<u8>) -> ArconResult<()> {
        let status = self.db.rmw(
            key,
            &FasterVecOps::Remove(to_remove),
            self.next_serial_number(),
        );

        match status {
            status::OK | status::PENDING => Ok(()),
            _ => arcon_err!("faster remove error: {}", status),
        }
    }

    fn vec_push(&mut self, key: &Vec<u8>, to_push: Vec<u8>) -> ArconResult<()> {
        let status = self
            .db
            .rmw(key, &FasterVecOps::Push(to_push), self.next_serial_number());

        match status {
            status::OK | status::PENDING => Ok(()),
            _ => arcon_err!("faster remove error: {}", status),
        }
    }

    fn vec_push_if_absent(&mut self, key: &Vec<u8>, to_push: Vec<u8>) -> ArconResult<()> {
        let status = self.db.rmw(
            key,
            &FasterVecOps::PushIfAbsent(to_push),
            self.next_serial_number(),
        );

        match status {
            status::OK | status::PENDING => Ok(()),
            _ => arcon_err!("faster remove error: {}", status),
        }
    }

    fn vec_set(&mut self, key: &Vec<u8>, value: &Vec<Vec<u8>>) -> ArconResult<()> {
        let status = self.db.upsert(key, value, self.next_serial_number());
        match status {
            status::OK | status::PENDING => Ok(()),
            _ => arcon_err!("faster put error: {}", status),
        }
    }

    fn aggregate(
        &mut self,
        key: &Vec<u8>,
        new: Vec<u8>,
        fun: &dyn Fn(&[u8], &[u8]) -> Vec<u8>,
    ) -> ArconResult<()> {
        let fun_fat_ptr_bytes = unsafe { std::mem::transmute(fun) };

        let status = self.db.rmw(
            key,
            &FasterAgg::Modify(new, fun_fat_ptr_bytes),
            self.next_serial_number(),
        );

        match status {
            status::OK | status::PENDING => Ok(()),
            _ => arcon_err!("faster put error: {}", status),
        }
    }

    fn get_agg(&self, key: &Vec<u8>) -> ArconResult<Option<Vec<u8>>> {
        let (status, receiver) = self.db.read(key, self.next_serial_number());
        match status {
            status::NOT_FOUND => Ok(None),
            status::OK | status::PENDING => {
                let vec_ops: FasterAgg = receiver
                    .recv_timeout(Duration::from_secs(2)) // TODO: make that customizable
                    .ctx("Could not receive result from faster thread")?;

                let val = match vec_ops {
                    FasterAgg::Value(v) => v,
                    FasterAgg::Modify(v, _fn_fat_ptr_bytes) => v,
                };

                Ok(Some(val))
            }
            _ => arcon_err!("faster get error: {}", status),
        }
    }
}

pub(crate) struct StateCommon<IK, N, KS, TS> {
    pub state_name: Vec<u8>,
    pub item_key: IK,
    pub namespace: N,
    pub key_serializer: KS,
    pub value_serializer: TS,
}

impl<IK, N, KS, TS> StateCommon<IK, N, KS, TS>
where
    IK: SerializableWith<KS>,
    N: SerializableWith<KS>,
{
    pub fn get_db_key_with_user_key<UK>(&self, user_key: &UK) -> ArconResult<Vec<u8>>
    where
        UK: SerializableWith<KS>,
    {
        let mut res = Vec::with_capacity(
            Vec::<u8>::size_hint(&LittleEndianBytesDump, &self.state_name).unwrap_or(0)
                + IK::size_hint(&self.key_serializer, &self.item_key).unwrap_or(0)
                + N::size_hint(&self.key_serializer, &self.namespace).unwrap_or(0)
                + UK::size_hint(&self.key_serializer, user_key).unwrap_or(0),
        );
        Vec::<u8>::serialize_into(&LittleEndianBytesDump, &mut res, &self.state_name)?;
        IK::serialize_into(&self.key_serializer, &mut res, &self.item_key)?;
        N::serialize_into(&self.key_serializer, &mut res, &self.namespace)?;
        UK::serialize_into(&self.key_serializer, &mut res, user_key)?;

        Ok(res)
    }

    pub fn get_db_key_prefix(&self) -> ArconResult<Vec<u8>> {
        let mut res = Vec::with_capacity(
            Vec::<u8>::size_hint(&LittleEndianBytesDump, &self.state_name).unwrap_or(0)
                + IK::size_hint(&self.key_serializer, &self.item_key).unwrap_or(0)
                + N::size_hint(&self.key_serializer, &self.namespace).unwrap_or(0),
        );
        Vec::<u8>::serialize_into(&LittleEndianBytesDump, &mut res, &self.state_name)?;
        IK::serialize_into(&self.key_serializer, &mut res, &self.item_key)?;
        N::serialize_into(&self.key_serializer, &mut res, &self.namespace)?;

        Ok(res)
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

impl<IK, N, T, KS, TS> ValueStateBuilder<IK, N, T, KS, TS> for Faster
where
    IK: SerializableWith<KS>,
    N: SerializableWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
{
    type Type = FasterValueState<IK, N, T, KS, TS>;

    fn new_value_state(
        &mut self,
        name: &str,
        item_key: IK,
        namespace: N,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type {
        FasterValueState {
            common: StateCommon {
                state_name: name.as_bytes().to_vec(),
                item_key,
                namespace,
                key_serializer,
                value_serializer,
            },
            _phantom: Default::default(),
        }
    }
}

impl<IK, N, K, V, KS, TS> MapStateBuilder<IK, N, K, V, KS, TS> for Faster
where
    IK: SerializableWith<KS> + DeserializableWith<KS>,
    N: SerializableWith<KS> + DeserializableWith<KS>,
    K: SerializableWith<KS> + DeserializableWith<KS>,
    V: SerializableWith<TS> + DeserializableWith<TS>,
    KS: Clone + 'static,
    TS: Clone + 'static,
{
    type Type = FasterMapState<IK, N, K, V, KS, TS>;

    fn new_map_state(
        &mut self,
        name: &str,
        item_key: IK,
        namespace: N,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type {
        FasterMapState {
            common: StateCommon {
                state_name: name.as_bytes().to_vec(),
                item_key,
                namespace,
                key_serializer,
                value_serializer,
            },
            _phantom: Default::default(),
        }
    }
}

impl<IK, N, T, KS, TS> VecStateBuilder<IK, N, T, KS, TS> for Faster
where
    IK: SerializableWith<KS>,
    N: SerializableWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
{
    type Type = FasterVecState<IK, N, T, KS, TS>;

    fn new_vec_state(
        &mut self,
        name: &str,
        item_key: IK,
        namespace: N,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type {
        FasterVecState {
            common: StateCommon {
                state_name: name.as_bytes().to_vec(),
                item_key,
                namespace,
                key_serializer,
                value_serializer,
            },
            _phantom: Default::default(),
        }
    }
}

impl<IK, N, T, F, KS, TS> ReducingStateBuilder<IK, N, T, F, KS, TS> for Faster
where
    IK: SerializableWith<KS>,
    N: SerializableWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
    F: Fn(&T, &T) -> T + Send + Sync + 'static,
    TS: Clone + Send + Sync + 'static,
{
    type Type = FasterReducingState<IK, N, T, F, KS, TS>;

    fn new_reducing_state(
        &mut self,
        name: &str,
        init_item_key: IK,
        init_namespace: N,
        reduce_fn: F,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type {
        FasterReducingState {
            common: StateCommon {
                state_name: name.as_bytes().to_vec(),
                item_key: init_item_key,
                namespace: init_namespace,
                key_serializer,
                value_serializer: value_serializer.clone(),
            },
            reduce_fn: reducing_state::make_reduce_fn(reduce_fn, value_serializer),
            _phantom: Default::default(),
        }
    }
}

impl<IK, N, T, AGG, KS, TS> AggregatingStateBuilder<IK, N, T, AGG, KS, TS> for Faster
where
    IK: SerializableWith<KS>,
    N: SerializableWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
    AGG: Aggregator<T> + Clone + Send + Sync + 'static,
    AGG::Accumulator: SerializableWith<TS> + DeserializableWith<TS>,
    TS: Clone + Send + Sync + 'static,
{
    type Type = FasterAggregatingState<IK, N, T, AGG, KS, TS>;

    fn new_aggregating_state(
        &mut self,
        name: &str,
        init_item_key: IK,
        init_namespace: N,
        aggregator: AGG,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type {
        FasterAggregatingState {
            common: StateCommon {
                state_name: name.as_bytes().to_vec(),
                item_key: init_item_key,
                namespace: init_namespace,
                key_serializer,
                value_serializer: value_serializer.clone(),
            },
            aggregator: aggregator.clone(),
            aggregate_fn: aggregating_state::make_aggregate_fn(aggregator, value_serializer),
            _phantom: Default::default(),
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
    use std::{
        fs,
        ops::{Deref, DerefMut},
    };
    use tempfile::TempDir;

    pub struct TestDb {
        faster: Faster,
        dir: TempDir,
    }

    impl TestDb {
        pub fn new() -> TestDb {
            let dir = TempDir::new().unwrap();
            let mut dir_path = dir.path().to_path_buf();
            dir_path.push("faster");
            fs::create_dir(&dir_path).unwrap();
            let faster = Faster::new(&dir_path).unwrap();
            TestDb { faster, dir }
        }

        pub fn checkpoint(&mut self) -> PathBuf {
            let mut checkpoint_dir = self.dir.path().to_path_buf();
            checkpoint_dir.push("checkpoint");
            self.faster.checkpoint(&checkpoint_dir).unwrap();
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
        type Target = Faster;

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
        let mut faster = Faster::new(dir.as_ref()).unwrap();

        faster.in_session_mut(|faster| {
            faster.put(&b"a".to_vec(), &b"1".to_vec()).unwrap();
            faster.put(&b"b".to_vec(), &b"2".to_vec()).unwrap();

            let one = faster.get(&b"a".to_vec()).unwrap().unwrap();
            let two = faster.get(&b"b".to_vec()).unwrap().unwrap();

            assert_eq!(&one, b"1");
            assert_eq!(&two, b"2");
        });

        let chkp_dir = tempfile::TempDir::new().unwrap();
        let restore_dir = tempfile::TempDir::new().unwrap();

        faster.checkpoint(chkp_dir.path()).unwrap();

        let mut restored = Faster::restore(restore_dir.path(), chkp_dir.path()).unwrap();

        assert!(!faster.was_restored());
        assert!(restored.was_restored());

        restored.in_session(|restored| {
            let one = restored.get(&b"a".to_vec()).unwrap().unwrap();
            let two = restored.get(&b"b".to_vec()).unwrap().unwrap();

            assert_eq!(&one, b"1");
            assert_eq!(&two, b"2");
        });

        restored.in_session_mut(|restored| {
            restored.remove(&b"a".to_vec()).unwrap();
            restored.put(&b"c".to_vec(), &b"3".to_vec()).unwrap();
        });

        let chkp2_dir = tempfile::TempDir::new().unwrap();
        let restore2_dir = tempfile::TempDir::new().unwrap();

        restored.checkpoint(chkp2_dir.path()).unwrap();

        let mut restored2 = Faster::restore(restore2_dir.path(), chkp2_dir.path()).unwrap();

        restored2.in_session_mut(|r2| {
            let one = r2.get(&b"a".to_vec()).unwrap();
            let two = r2.get(&b"b".to_vec()).unwrap().unwrap();
            let three = r2.get(&b"c".to_vec()).unwrap().unwrap();

            assert_eq!(one, None);
            assert_eq!(&two, b"2");
            assert_eq!(&three, b"3");
        })
    }
}
