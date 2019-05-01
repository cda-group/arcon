extern crate rocksdb;

use rocksdb::{Options, DB};

use crate::error::ErrorKind::*;
use crate::error::*;
use crate::StateBackend;

pub struct RocksDB {
    db: DB,
    storage_dir: String,
}

impl StateBackend for RocksDB {
    fn create(name: &str) -> RocksDB {
        RocksDB {
            db: DB::open_default(&name).expect("Failed to open RocksDB"),
            storage_dir: name.to_string(),
        }
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db
            .put(key, value)
            .map_err(|e| Error::new(PutError(e.to_string())))
    }

    fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        match self.db.get(key) {
            Ok(Some(value)) => Ok(value.to_vec()),
            Ok(None) => Err(Error::new(GetError("value not found".to_string()))),
            Err(e) => Err(Error::new(GetError(e.to_string()))),
        }
    }

    fn checkpoint(&self, id: String) -> Result<()> {
        let checkpoint_path = format!("{}/checkpoints/{}", self.storage_dir, id);
        let path = std::path::Path::new(&checkpoint_path);
        let _ = rocksdb::checkpoint::Checkpoint::new(&self.db)
            .map(|c| c.create_checkpoint(path))
            .map_err(|e| Error::new(CheckpointError(e.to_string())))?;
        Ok(())
    }
}

impl RocksDB {
    pub fn with_config(options: Options, name: String) -> RocksDB {
        RocksDB {
            db: DB::open(&options, &name).unwrap(),
            storage_dir: name.to_string(),
        }
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
        let mut db = RocksDB::create(&dir_path);

        let key = "key";
        let value = "test";

        db.put(key.as_bytes(), value.as_bytes()).expect("put");

        let v = db.get(key.as_bytes()).unwrap();
        assert_eq!(value, String::from_utf8_lossy(&v));
    }
}
