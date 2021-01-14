// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{Key, Metakey, Value},
    error::*,
    Aggregator, AggregatorState, Backend, Handle, MapState, Reducer, ReducerState, ValueState,
    VecState,
};
use sled::{open, Batch, Db, IVec, Tree};
use std::path::Path;
#[cfg(feature = "sled_checkpoints")]
use std::{
    fs, io,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

#[derive(Debug)]
pub struct Sled {
    db: Db,
    restored: bool,
}

impl Sled {
    fn tree(&self, tree_name: &str) -> Result<Tree> {
        Ok(self.db.open_tree(tree_name)?)
    }

    fn get(&self, tree_name: &str, key: &[u8]) -> Result<Option<IVec>> {
        let tree = self.tree(tree_name)?;
        let val = tree.get(key)?;
        Ok(val)
    }

    fn put(&self, tree_name: &str, key: &[u8], value: &[u8]) -> Result<Option<IVec>> {
        let tree = self.tree(tree_name)?;
        let old = tree.insert(key, value)?;
        Ok(old)
    }

    fn remove(&self, tree_name: &str, key: &[u8]) -> Result<Option<IVec>> {
        let tree = self.tree(tree_name)?;
        let old = tree.remove(key)?;
        Ok(old)
    }

    fn remove_prefix(&self, tree_name: &str, prefix: Vec<u8>) -> Result<()> {
        let tree = self.tree(tree_name)?;

        let mut batch = Batch::default();
        for key in tree.scan_prefix(prefix).keys() {
            batch.remove(key?);
        }

        tree.apply_batch(batch)?;

        Ok(())
    }

    fn contains(&self, tree_name: &str, key: &[u8]) -> Result<bool> {
        let tree = self.tree(tree_name)?;
        Ok(tree.contains_key(key)?)
    }
}

impl Backend for Sled {
    fn create(live_path: &Path) -> Result<Self>
    where
        Self: Sized,
    {
        let db = open(live_path)?;
        Ok(Sled {
            db,
            restored: false,
        })
    }

    #[allow(unused_variables)]
    fn restore(live_path: &Path, checkpoint_path: &Path) -> Result<Self>
    where
        Self: Sized,
    {
        let db = open(live_path)?;
        #[allow(unused_assignments, unused_mut)]
        let mut restored = false;

        #[cfg(feature = "sled_checkpoints")]
        {
            let mut p: PathBuf = checkpoint_path.into();
            p.push("SLED_EXPORT");
            let import_data = parse_dumped_sled_export(&p)?;
            db.import(import_data);

            restored = true;
        }

        Ok(Sled { db, restored })
    }

    fn was_restored(&self) -> bool {
        self.restored
    }

    #[cfg(not(feature = "sled_checkpoints"))]
    fn checkpoint(&self, _checkpoint_path: &Path) -> Result<()> {
        eprintln!(
            "Checkpointing sled state backends is off (compiled without `arcon_sled_checkpoints`)"
        );
        Ok(())
    }

    #[cfg(feature = "sled_checkpoints")]
    fn checkpoint(&self, checkpoint_path: &Path) -> Result<()> {
        // TODO: sled doesn't support checkpoints/snapshots, but that and MVCC is planned
        //   for now we'll just dump it via the export/import mechanism WHICH MAY BE VERY SLOW
        let export_data = self.db.export();

        let mut p: PathBuf = checkpoint_path.into();
        if !p.exists() {
            fs::create_dir_all(&p)?;
        }

        p.push("SLED_EXPORT");
        let out = fs::File::create(&p)?;
        let mut writer = BufWriter::new(out);

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
    }

    fn register_value_handle<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &mut Handle<ValueState<T>, IK, N>,
    ) {
        handle.registered = true;
    }

    fn register_map_handle<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &mut Handle<MapState<K, V>, IK, N>,
    ) {
        handle.registered = true;
    }

    fn register_vec_handle<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &mut Handle<VecState<T>, IK, N>,
    ) {
        let tree = self
            .tree(&handle.id)
            .expect("Could not get the tree when registering a vec");
        tree.set_merge_operator(vec_ops::vec_merge);
        handle.registered = true;
    }

    fn register_reducer_handle<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &self,
        handle: &mut Handle<ReducerState<T, F>, IK, N>,
    ) {
        let tree = self
            .tree(&handle.id)
            .expect("Could not get the tree when registering a reducer");
        tree.set_merge_operator(reducer_ops::make_reducer_merge(handle.extra_data.clone()));
        handle.registered = true;
    }

    fn register_aggregator_handle<A: Aggregator, IK: Metakey, N: Metakey>(
        &self,
        handle: &mut Handle<AggregatorState<A>, IK, N>,
    ) {
        let tree = self
            .tree(&handle.id)
            .expect("Could not get the tree when registering an aggregator");
        tree.set_merge_operator(aggregator_ops::make_aggregator_merge(
            handle.extra_data.clone(),
        ));
        handle.registered = true;
    }
}

mod aggregator_ops;
mod map_ops;
mod reducer_ops;
mod value_ops;
mod vec_ops;

#[cfg(feature = "sled_checkpoints")]
fn parse_dumped_sled_export(
    dump_path: &Path,
) -> Result<Vec<(Vec<u8>, Vec<u8>, impl Iterator<Item = Vec<Vec<u8>>>)>> {
    let f = fs::File::open(dump_path)?;
    let mut reader = BufReader::new(f);

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        ops::{Deref, DerefMut},
        sync::Arc,
    };
    use tempfile::TempDir;

    #[derive(Debug)]
    pub struct TestDb {
        sled: Arc<Sled>,
        dir: TempDir,
    }

    impl TestDb {
        pub fn new() -> TestDb {
            let dir = TempDir::new().unwrap();
            let mut dir_path = dir.path().to_path_buf();
            dir_path.push("sled");
            fs::create_dir(&dir_path).unwrap();
            let sled = Sled::create(&dir_path).unwrap();
            TestDb {
                sled: Arc::new(sled),
                dir,
            }
        }
    }

    impl Deref for TestDb {
        type Target = Arc<Sled>;

        fn deref(&self) -> &Self::Target {
            &self.sled
        }
    }

    impl DerefMut for TestDb {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.sled
        }
    }

    #[cfg(feature = "sled_checkpoints")]
    #[test]
    fn test_sled_checkpoints() {
        let dir = TempDir::new().unwrap();
        let sled = Sled::create(dir.path()).unwrap();

        sled.db.insert(b"a", b"1").unwrap();
        sled.db.insert(b"b", b"2").unwrap();

        let t = sled.db.open_tree(b"other tree").unwrap();
        t.insert(b"x", b"10").unwrap();
        t.insert(b"y", b"20").unwrap();

        let chkp_dir = TempDir::new().unwrap();
        let restore_dir = TempDir::new().unwrap();

        sled.checkpoint(chkp_dir.path()).unwrap();

        let restored = Sled::restore(restore_dir.path(), chkp_dir.path()).unwrap();

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

    common_state_tests!(TestDb::new());
}
