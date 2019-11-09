extern crate rocksdb;

use rocksdb::Writable;
use rocksdb::DB;

use crate::state_backend::StateBackend;
use arcon_error::*;

pub struct RocksDB {
    db: DB,
}

impl StateBackend for RocksDB {
    fn create(name: &str) -> ArconResult<RocksDB> {
        let db = DB::open_default(&name)
            .map_err(|e| arcon_err_kind!("Failed to create RocksDB instance: {}", e.to_string()))?;
        Ok(RocksDB { db })
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> ArconResult<()> {
        self.db
            .put(key, value)
            .map_err(|e| arcon_err_kind!("RocksDB put err: {}", e.to_string()))
    }

    fn get(&self, key: &[u8]) -> ArconResult<Vec<u8>> {
        match self.db.get(key) {
            Ok(Some(data)) => Ok(data.to_vec()),
            Ok(None) => arcon_err!("{}", "Value not found"),
            Err(e) => arcon_err!("{}", e.to_string()),
        }
    }

    fn checkpoint(&self, _id: String) -> ArconResult<()> {
        unimplemented!();
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
        let mut db = RocksDB::create(&dir_path).unwrap();

        let key = "key";
        let value = "test";

        db.put(key.as_bytes(), value.as_bytes()).expect("put");

        let v = db.get(key.as_bytes()).unwrap();
        assert_eq!(value, String::from_utf8_lossy(&v));
    }
}
