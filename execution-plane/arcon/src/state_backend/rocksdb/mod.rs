// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

extern crate rocksdb;

use arcon_error::*;
use serde::{Serialize, Deserialize};
use rocksdb::{
    checkpoint::Checkpoint,
    DB,
    ColumnFamilyDescriptor,
};
use std::{
    path::{PathBuf, Path},
    io::Write,
};
use crate::state_backend::{StateBackend, ValueStateBuilder};
use self::rocksdb::{Options, ColumnFamily, DBPinnableSlice};
use std::error::Error;
use crate::state_backend::rocksdb::value_state::RocksDbValueState;

pub struct RocksDb {
    db: DB,
    checkpoints_path: PathBuf,
}

impl RocksDb {
    fn get_cf(&self, cf_name: impl AsRef<str>) -> ArconResult<&ColumnFamily> {
        self.db.cf_handle(cf_name.as_ref())
            .ok_or_else(|| arcon_err_kind!("Could not get column family '{}'", cf_name.as_ref()))
    }

    fn set_checkpoints_path<P: AsRef<Path>>(&mut self, path: P) {
        self.checkpoints_path = path.as_ref().to_path_buf();
    }

    fn get(&self, cf_name: impl AsRef<str>, key: impl AsRef<[u8]>) -> ArconResult<DBPinnableSlice> {
        let cf = self.get_cf(cf_name)?;

        match self.db.get_pinned_cf(cf, key) {
            Ok(Some(data)) => Ok(data),
            Ok(None) => arcon_err!("{}", "Value not found"),
            Err(e) => arcon_err!("{}", e),
        }
    }

    fn put(&mut self, cf_name: impl AsRef<str>, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> ArconResult<()> {
        let cf = self.get_cf(cf_name)?;

        self.db.put_cf(cf, key, value)
            .map_err(|e| arcon_err_kind!("RocksDB put err: {}", e))
    }

    fn remove(&mut self, cf: impl AsRef<str>, key: impl AsRef<[u8]>) -> ArconResult<()> {
        let cf = self.get_cf(cf)?;
        self.db.delete_cf(cf, key)
            .map_err(|e| arcon_err_kind!("RocksDB delete err: {}", e))
    }

    // TODO: maybe add some state type enum to this?
    fn get_or_create_column_family(&mut self, state_name: &str) -> String {
        if self.db.cf_handle(state_name).is_none() {
            self.db.create_cf(state_name, &Options::default());
        }

        state_name.to_string()
    }

    fn create_state_common<IK, N>(&mut self, state_name: &str, curr_key: IK, curr_namespace: N) -> StateCommon<IK, N> {
        let cf_name = self.get_or_create_column_family(state_name);
        StateCommon { cf_name, curr_key, curr_namespace }
    }
}

impl StateBackend for RocksDb {
    fn new(name: &str) -> ArconResult<RocksDb> {
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
        let common = self.create_state_common(name, init_item_key, init_namespace);
        RocksDbValueState { common, _phantom: Default::default() }
    }
}

pub(crate) struct StateCommon<IK, N> {
    cf_name: String,
    // TODO: decide if we need a full descriptor, or just a name
    curr_key: IK,
    curr_namespace: N,
}

impl<IK, N> StateCommon<IK, N> where IK: Serialize, N: Serialize {
    fn get_db_key<UK>(&self, user_key: &UK) -> ArconResult<Vec<u8>> where UK: Serialize {
        let res = bincode::serialize(&(
            &self.curr_key,
            &self.curr_namespace,
            user_key
        )).map_err(|e| arcon_err_kind!("Could not serialize keys and namespace: {}", e))?;

        Ok(res)
    }
}

mod value_state;
//mod map_state; TODO
//mod vec_state; TODO
//mod reducing_state; TODO
//mod aggregating_state; TODO

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_unit_state_key_empty() {
        let state = StateCommon {
            cf_name: "".to_string(),
            curr_key: (),
            curr_namespace: ()
        };

        let v = state.get_db_key(&()).unwrap();

        assert!(v.is_empty());
    }

    #[test]
    fn simple_rocksdb_test() {
        let tmp_dir = TempDir::new().unwrap();
        let dir_path = tmp_dir.path().to_string_lossy().into_owned();
        let mut db = RocksDb::new(&dir_path).unwrap();

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
