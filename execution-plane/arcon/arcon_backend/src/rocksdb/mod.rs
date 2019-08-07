extern crate rocksdb;

use rocksdb::{Options, DB};

use crate::StateBackend;
use arcon_error::*;

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

    fn put(&mut self, key: &[u8], value: &[u8]) -> ArconResult<()> {
        self.db
            .put(key, value)
            .map_err(|e| arcon_err_kind!("RocksDB put err: {}", e.to_string()))
    }

    fn get(&self, key: &[u8]) -> ArconResult<Vec<u8>> {
        match self.db.get(key) {
            Ok(Some(value)) => Ok(value.to_vec()),
            Ok(None) => arcon_err!("{}", "Value not found"),
            Err(e) => arcon_err!("{}", e.to_string()),
        }
    }

    fn checkpoint(&self, id: String) -> ArconResult<()> {
        // TODO: yeah, fix this..
        let checkpoint_path = format!("{}/checkpoints/{}", self.storage_dir, id);
        let path = std::path::Path::new(&checkpoint_path);
        let _ = rocksdb::checkpoint::Checkpoint::new(&self.db)
            .map(|c| c.create_checkpoint(path))
            .map_err(|e| arcon_err_kind!("Checkpoint err: {}", e.to_string()))?;
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
