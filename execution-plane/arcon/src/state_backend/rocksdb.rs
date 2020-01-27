// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

extern crate rocksdb;

use rocksdb::DB;

use crate::state_backend::StateBackend;
use arcon_error::*;
use std::path::{PathBuf, Path};
use self::rocksdb::checkpoint::Checkpoint;
use crate::state_backend::state_types::{ReducingState, ValueState, MapState, VecState, AggregatingState};
use std::rc::Rc;
use std::cell::RefCell;

pub struct RocksDB {
    inner: Rc<RefCell<Inner>>
}

struct Inner {
    db: DB,
    checkpoints_path: PathBuf,
}

impl RocksDB {
    fn set_checkpoints_path<P: AsRef<Path>>(&mut self, path: P) {
        self.inner.borrow_mut().checkpoints_path = path.as_ref().to_path_buf();
    }
}

impl StateBackend for RocksDB {
    fn new(name: &str) -> ArconResult<RocksDB> {
        let db = DB::open_default(&name)
            .map_err(|e| arcon_err_kind!("Failed to create RocksDB instance: {}", e))?;
        let checkpoints_path = PathBuf::from(format!("{}-checkpoints", name));
        Ok(RocksDB { inner: Rc::new(RefCell::new(Inner { db, checkpoints_path }))})
    }

    fn get_cloned(&self, key: &[u8]) -> ArconResult<Vec<u8>> {
        match self.inner.borrow().db.get(key) {
            Ok(Some(data)) => Ok(data.to_vec()),
            Ok(None) => arcon_err!("{}", "Value not found"),
            Err(e) => arcon_err!("{}", e),
        }
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> ArconResult<()> {
        self.inner.borrow_mut().db.put(key, value)
            .map_err(|e| arcon_err_kind!("RocksDB put err: {}", e))
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), Error> {
        self.inner.borrow_mut().db.delete(key)
            .map_err(|e| arcon_err_kind!("RocksDB delete err: {}", e))
    }

    fn checkpoint(&self, id: String) -> ArconResult<()> {
        let this = self.inner.borrow();
        let checkpointer = Checkpoint::new(&this.db)
            .map_err(|e| arcon_err_kind!("Could not create checkpoint object: {}", e))?;

        let mut path = this.checkpoints_path.clone();
        path.push(id);

        this.db.flush()
            .map_err(|e| arcon_err_kind!("Could not flush rocksdb: {}", e))?;

        checkpointer.create_checkpoint(path)
            .map_err(|e| arcon_err_kind!("Could not save the checkpoint: {}", e))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn simple_rocksdb_test() {
        let tmp_dir = TempDir::new().unwrap();
        let dir_path = tmp_dir.path().to_string_lossy().into_owned();
        let mut db = RocksDB::new(&dir_path).unwrap();

        let key = "key";
        let value = "test";

        db.put(key.as_bytes(), value.as_bytes()).expect("put");

        let v = db.get_cloned(key.as_bytes()).unwrap();
        assert_eq!(value, String::from_utf8_lossy(&v));

        db.remove(key.as_bytes()).expect("remove");
        let v = db.get_cloned(key.as_bytes());
        assert!(v.is_err());
    }

    #[test]
    fn checkpoint_rocksdb_test() {
        let tmp_dir = TempDir::new().unwrap();
        let checkpoints_dir = TempDir::new().unwrap();

        let dir_path = tmp_dir.path().to_string_lossy();
        let checkpoints_dir_path = checkpoints_dir.path().to_string_lossy();

        let mut db = RocksDB::new(&dir_path).unwrap();
        db.set_checkpoints_path(checkpoints_dir_path.as_ref());

        let key: &[u8] = b"key";
        let initial_value: &[u8] = b"value";
        let new_value: &[u8] = b"new value";

        db.put(key, initial_value).expect("put failed");
        db.checkpoint("chkpt0".into()).expect("checkpoint failed");
        db.put(key, new_value).expect("second put failed");

        let mut last_checkpoint_path = checkpoints_dir.path().to_owned();
        last_checkpoint_path.push("chkpt0");

        let db_from_checkpoint = RocksDB::new(&last_checkpoint_path.to_string_lossy())
            .expect("Could not open checkpointed db");

        assert_eq!(
            new_value,
            db.get_cloned(key)
                .expect("Could not get from the original db")
                .as_slice()
        );
        assert_eq!(
            initial_value,
            db_from_checkpoint.get_cloned(key)
                .expect("Could not get from the checkpoint")
                .as_slice()
        );
    }
}
