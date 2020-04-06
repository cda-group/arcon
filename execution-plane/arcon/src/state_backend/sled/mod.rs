use crate::{
    prelude::{
        AggregatingStateBuilder, ArconResult, ReducingStateBuilder, ValueStateBuilder,
        VecStateBuilder,
    },
    state_backend::{
        serialization::{DeserializableWith, SerializableFixedSizeWith, SerializableWith},
        sled::{
            aggregating_state::SledAggregatingState, map_state::SledMapState,
            reducing_state::SledReducingState, value_state::SledValueState,
            vec_state::SledVecState,
        },
        state_types::Aggregator,
        MapStateBuilder, StateBackend,
    },
};
use ::sled::{open, Db};
use error::ResultExt;
use sled::{Batch, IVec, Tree};
use std::{
    fs::{create_dir_all, File},
    io,
    io::{BufReader, Read},
};
#[cfg(feature = "arcon_sled_checkpoints")]
use std::{
    io::{BufWriter, Seek, SeekFrom, Write},
    path::PathBuf,
};

pub struct Sled {
    db: Db,
    restored: bool,
}

impl StateBackend for Sled {
    fn new(path: &str) -> ArconResult<Self>
    where
        Self: Sized,
    {
        let db = open(path).ctx("Could not open sled")?;
        Ok(Sled {
            db,
            restored: false,
        })
    }

    #[cfg(not(feature = "arcon_sled_checkpoints"))]
    fn checkpoint(&self, _checkpoint_path: &str) -> ArconResult<()> {
        eprintln!(
            "Checkpointing sled state backends is off (compiled without `arcon_sled_checkpoints`)"
        );
        Ok(())
    }

    #[cfg(feature = "arcon_sled_checkpoints")]
    fn checkpoint(&self, checkpoint_path: &str) -> ArconResult<()> {
        // TODO: sled doesn't support checkpoints/snapshots, but that and MVCC is planned
        //   for now we'll just dump it via the export/import mechanism WHICH MAY BE VERY SLOW
        let export_data = self.db.export();

        let mut p: PathBuf = checkpoint_path.into();
        if !p.exists() {
            create_dir_all(&p).ctx("could not create checkpoint directory")?;
        }

        p.push("SLED_EXPORT");
        let out = File::create(&p).ctx("Could not create checkpoint file")?;
        let mut writer = BufWriter::new(out);

        // TODO: the following section has a lot of io::Result-returning methods, and `try` expressions
        //   aren't stable yet, so let's do immediately-executed closure trick
        || -> io::Result<()> {
            writer.write_all(&export_data.len().to_le_bytes())?;

            #[inline]
            fn write_len_and_bytes(mut w: impl Write, bytes: &[u8]) -> io::Result<()> {
                w.write_all(&bytes.len().to_le_bytes())?;
                w.write_all(bytes)?;
                Ok(())
            }

            for (typ, name, vvecs) in export_data {
                write_len_and_bytes(&mut writer, &typ)?;
                write_len_and_bytes(&mut writer, &name)?;

                // we don't know how many elements are in `vvecs`, so let's save the current position,
                // write a dummy length, write all the elements and then go back and write how many we wrote
                let length_spot = writer.seek(SeekFrom::Current(0))?;
                writer.write_all(&0usize.to_le_bytes())?;
                let mut length = 0usize;

                for vecs in vvecs {
                    length += 1;
                    writer.write_all(&vecs.len().to_le_bytes())?;
                    for vec in vecs {
                        write_len_and_bytes(&mut writer, &vec)?;
                    }
                }

                let after_vecs = writer.seek(SeekFrom::Current(0))?;
                let _ = writer.seek(SeekFrom::Start(length_spot))?;
                writer.write_all(&length.to_le_bytes())?;
                let _ = writer.seek(SeekFrom::Start(after_vecs))?;
            }
            Ok(())
        }()
        .ctx("sled checkpoint io error")?;

        Ok(())
    }

    #[allow(unused_variables)]
    fn restore(restore_path: &str, checkpoint_path: &str) -> ArconResult<Self>
    where
        Self: Sized,
    {
        let db = open(restore_path).ctx("Could open sled")?;
        #[allow(unused_assignments, unused_mut)]
        let mut restored = false;

        #[cfg(feature = "arcon_sled_checkpoints")]
        {
            let mut p: PathBuf = checkpoint_path.into();
            p.push("SLED_EXPORT");
            let import_data = parse_dumped_sled_export(p.to_string_lossy().as_ref())?;
            db.import(import_data);

            restored = true;
        }

        Ok(Sled { db, restored })
    }

    fn was_restored(&self) -> bool {
        self.db.was_recovered() || self.restored
    }
}

#[allow(dead_code)]
fn parse_dumped_sled_export(
    dump_path: &str,
) -> ArconResult<Vec<(Vec<u8>, Vec<u8>, impl Iterator<Item = Vec<Vec<u8>>>)>> {
    let f = File::open(dump_path).ctx("Sled restore error")?;
    let mut reader = BufReader::new(f);

    // immediately executed closure trick, TODO: use `try` expression when it stabilises
    let res = || -> io::Result<_> {
        #[inline]
        fn read_length(r: &mut impl Read) -> io::Result<usize> {
            let mut length_bytes = 0usize.to_le_bytes();
            r.read_exact(&mut length_bytes)?;
            Ok(usize::from_le_bytes(length_bytes))
        }

        let length = read_length(&mut reader)?;
        let mut res = Vec::with_capacity(length);

        #[inline]
        fn read_bytes_starting_with_len(r: &mut impl Read) -> io::Result<Vec<u8>> {
            let length = read_length(r)?;
            let mut res = vec![0u8; length];
            r.read_exact(&mut res)?;
            Ok(res)
        }

        // TODO: can we make it so this doesn't run out of memory when the database is huge?
        //   that could be possibly done by skipping around the file to read the type and name and
        //   then reading the actual contents (impl Iterator) lazily
        for _ in 0..length {
            let typ = read_bytes_starting_with_len(&mut reader)?;
            let name = read_bytes_starting_with_len(&mut reader)?;

            let vvecs_length = read_length(&mut reader)?;
            let mut vvecs = Vec::with_capacity(vvecs_length);
            for _ in 0..vvecs_length {
                let vecs_length = read_length(&mut reader)?;
                let mut vecs = Vec::with_capacity(vecs_length);
                for _ in 0..vecs_length {
                    vecs.push(read_bytes_starting_with_len(&mut reader)?);
                }
                vvecs.push(vecs);
            }
            let vvecs = vvecs.into_iter();

            res.push((typ, name, vvecs))
        }

        Ok(res)
    }()
    .ctx("sled restore io error")?;

    Ok(res)
}

impl Sled {
    fn tree(&self, tree_name: &[u8]) -> ArconResult<Tree> {
        Ok(self
            .db
            .open_tree(tree_name)
            .ctx("Could not find the correct sled tree")?)
    }

    fn get(&self, tree_name: &[u8], key: &[u8]) -> ArconResult<Option<IVec>> {
        let tree = self.tree(tree_name)?;

        let val = tree.get(key).ctx("Could not get value")?;

        Ok(val)
    }

    fn put(&mut self, tree_name: &[u8], key: &[u8], value: &[u8]) -> ArconResult<Option<IVec>> {
        let tree = self.tree(tree_name)?;
        let old = tree.insert(key, value).ctx("Could not insert value")?;
        Ok(old)
    }

    fn remove(&mut self, tree_name: &[u8], key: &[u8]) -> ArconResult<()> {
        let tree = self.tree(tree_name)?;
        tree.remove(key).ctx("Could not remove")?;
        Ok(())
    }

    fn remove_prefix(&mut self, tree_name: &[u8], prefix: Vec<u8>) -> ArconResult<()> {
        let tree = self.tree(tree_name)?;

        let mut batch = Batch::default();
        for key in tree.scan_prefix(prefix).keys() {
            batch.remove(key.ctx("Could not get key from sled iterator")?);
        }

        tree.apply_batch(batch).ctx("Could not batch remove")?;

        Ok(())
    }

    fn contains(&self, tree_name: &[u8], key: &[u8]) -> ArconResult<bool> {
        let tree = self.tree(tree_name)?;
        tree.contains_key(key)
            .ctx("Could not check if tree contains the given key")
    }
}

pub(crate) struct StateCommon<IK, N, KS, TS> {
    pub tree_name: Vec<u8>,
    pub item_key: IK,
    pub namespace: N,
    pub key_serializer: KS,
    pub value_serializer: TS,
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
}

impl<IK, N, T, KS, TS> ValueStateBuilder<IK, N, T, KS, TS> for Sled
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
{
    type Type = SledValueState<IK, N, T, KS, TS>;

    fn new_value_state(
        &mut self,
        name: &str,
        item_key: IK,
        namespace: N,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type {
        SledValueState {
            common: StateCommon {
                tree_name: name.as_bytes().to_vec(),
                item_key,
                namespace,
                key_serializer,
                value_serializer,
            },
            _phantom: Default::default(),
        }
    }
}

impl<IK, N, K, V, KS, TS> MapStateBuilder<IK, N, K, V, KS, TS> for Sled
where
    IK: SerializableFixedSizeWith<KS> + DeserializableWith<KS>,
    N: SerializableFixedSizeWith<KS> + DeserializableWith<KS>,
    K: SerializableWith<KS> + DeserializableWith<KS>,
    V: SerializableWith<TS> + DeserializableWith<TS>,
    KS: Clone + 'static,
    TS: Clone + 'static,
{
    type Type = SledMapState<IK, N, K, V, KS, TS>;

    fn new_map_state(
        &mut self,
        name: &str,
        item_key: IK,
        namespace: N,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type {
        SledMapState {
            common: StateCommon {
                tree_name: name.as_bytes().to_vec(),
                item_key,
                namespace,
                key_serializer,
                value_serializer,
            },
            _phantom: Default::default(),
        }
    }
}

impl<IK, N, T, KS, TS> VecStateBuilder<IK, N, T, KS, TS> for Sled
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS>,
{
    type Type = SledVecState<IK, N, T, KS, TS>;

    fn new_vec_state(
        &mut self,
        name: &str,
        init_item_key: IK,
        init_namespace: N,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type {
        // TODO: the below could panic. Should we change the signature in the builder traits?
        let tree = self
            .tree(name.as_bytes())
            .expect("Could not get the required tree");
        tree.set_merge_operator(self::vec_state::vec_merge);

        SledVecState {
            common: StateCommon {
                tree_name: name.as_bytes().to_vec(),
                item_key: init_item_key,
                namespace: init_namespace,
                key_serializer,
                value_serializer,
            },
            _phantom: Default::default(),
        }
    }
}

impl<IK, N, T, F, KS, TS> ReducingStateBuilder<IK, N, T, F, KS, TS> for Sled
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    // TODO: the 'static part of this doesn't seem right, but it doesn't compile otherwise
    T: SerializableWith<TS> + DeserializableWith<TS> + 'static,
    F: Fn(&T, &T) -> T + Send + Sync + Clone + 'static,
    TS: Send + Sync + Clone + 'static,
{
    type Type = SledReducingState<IK, N, T, F, KS, TS>;

    fn new_reducing_state(
        &mut self,
        name: &str,
        init_item_key: IK,
        init_namespace: N,
        reduce_fn: F,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type {
        let tree = self
            .tree(name.as_bytes())
            .expect("Could not get the required tree");

        tree.set_merge_operator(reducing_state::make_reducing_merge(
            reduce_fn.clone(),
            value_serializer.clone(),
        ));

        SledReducingState {
            common: StateCommon {
                tree_name: name.as_bytes().to_vec(),
                item_key: init_item_key,
                namespace: init_namespace,
                key_serializer,
                value_serializer,
            },
            reduce_fn,
            _phantom: Default::default(),
        }
    }
}

impl<IK, N, T, AGG, KS, TS> AggregatingStateBuilder<IK, N, T, AGG, KS, TS> for Sled
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: SerializableWith<TS> + DeserializableWith<TS> + 'static,
    AGG: Aggregator<T> + Send + Sync + Clone + 'static,
    AGG::Accumulator: SerializableWith<TS> + DeserializableWith<TS>,
    TS: Send + Sync + Clone + 'static,
{
    type Type = SledAggregatingState<IK, N, T, AGG, KS, TS>;

    fn new_aggregating_state(
        &mut self,
        name: &str,
        init_item_key: IK,
        init_namespace: N,
        aggregator: AGG,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type {
        let tree = self
            .tree(name.as_bytes())
            .expect("Could not get the required tree");
        tree.set_merge_operator(aggregating_state::make_aggregating_merge(
            aggregator.clone(),
            value_serializer.clone(),
        ));

        SledAggregatingState {
            common: StateCommon {
                tree_name: name.as_bytes().to_vec(),
                item_key: init_item_key,
                namespace: init_namespace,
                key_serializer,
                value_serializer,
            },
            aggregator,
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
    use sled::IVec;
    use std::{
        fs,
        ops::{Deref, DerefMut},
    };
    use tempfile::TempDir;

    pub struct TestDb {
        sled: Sled,
        dir: TempDir,
    }

    impl TestDb {
        pub fn new() -> TestDb {
            let dir = TempDir::new().unwrap();
            let mut dir_path = dir.path().to_path_buf();
            dir_path.push("sled");
            fs::create_dir(&dir_path).unwrap();
            let dir_path = dir_path.to_string_lossy();
            let sled = Sled::new(&dir_path).unwrap();
            TestDb { sled, dir }
        }

        pub fn checkpoint(&mut self) -> PathBuf {
            let mut checkpoint_dir = self.dir.path().to_path_buf();
            checkpoint_dir.push("checkpoint");
            self.sled
                .checkpoint(&checkpoint_dir.to_string_lossy())
                .unwrap();
            checkpoint_dir
        }

        pub fn from_checkpoint(checkpoint_dir: &str) -> TestDb {
            let dir = TempDir::new().unwrap();
            let mut dir_path = dir.path().to_path_buf();
            dir_path.push("sled");
            let dir_path = dir_path.to_string_lossy();
            let sled = Sled::restore(checkpoint_dir, &dir_path).unwrap();
            TestDb { sled, dir }
        }
    }

    impl Deref for TestDb {
        type Target = Sled;

        fn deref(&self) -> &Self::Target {
            &self.sled
        }
    }

    impl DerefMut for TestDb {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.sled
        }
    }

    #[cfg(feature = "arcon_sled_checkpoints")]
    #[test]
    fn test_sled_checkpoints() {
        let dir = tempfile::TempDir::new().unwrap();
        let sled = Sled::new(dir.path().to_string_lossy().as_ref()).unwrap();

        sled.db.insert(b"a", b"1").unwrap();
        sled.db.insert(b"b", b"2").unwrap();

        let t = sled.db.open_tree(b"other tree").unwrap();
        t.insert(b"x", b"10").unwrap();
        t.insert(b"y", b"20").unwrap();

        let chkp_dir = tempfile::TempDir::new().unwrap();
        let restore_dir = tempfile::TempDir::new().unwrap();

        sled.checkpoint(chkp_dir.path().to_string_lossy().as_ref())
            .unwrap();

        let restored = Sled::restore(
            restore_dir.path().to_string_lossy().as_ref(),
            chkp_dir.path().to_string_lossy().as_ref(),
        )
        .unwrap();

        assert!(!sled.was_restored());
        assert!(restored.was_restored());

        assert_eq!(restored.db.len(), 2);
        assert_eq!(restored.db.get(b"a"), Ok(Some(IVec::from(b"1"))));
        assert_eq!(restored.db.get(b"b"), Ok(Some(IVec::from(b"2"))));

        assert_eq!(restored.db.tree_names().len(), 2);
        assert!(restored
            .db
            .tree_names()
            .contains(&IVec::from(b"other tree")));

        let restored_t = restored.db.open_tree(b"other tree").unwrap();
        assert_eq!(restored_t.len(), 2);
        assert_eq!(restored_t.get(b"x"), Ok(Some(IVec::from(b"10"))));
        assert_eq!(restored_t.get(b"y"), Ok(Some(IVec::from(b"20"))));
    }
}
