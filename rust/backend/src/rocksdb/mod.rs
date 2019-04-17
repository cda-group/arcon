extern crate rocksdb;

use rocksdb::{DBOptions, DBVector, Writable, DB};

use crate::error::ErrorKind::*;
use crate::error::*;
use crate::state_backend::StateBackend;

pub struct RocksDB {
    db: DB,
}

impl StateBackend for RocksDB {
    fn create(name: &str) -> RocksDB {
        RocksDB {
            db: DB::open_default(&name).unwrap(),
        }
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db
            .put(key, value)
            .map_err(|e| Error::new(PutError("failed putting value".to_string())))
    }

    fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        match self.db.get(key) {
            Ok(Some(value)) => Ok(value.to_vec()),
            Ok(None) => Err(Error::new(GetError("value not found".to_string()))),
            Err(e) => Err(Error::new(GetError("something went wrong".to_string()))),
        }
    }
}

impl RocksDB {
    pub fn with_config(options: DBOptions, name: String) -> RocksDB {
        RocksDB {
            db: DB::open(options, &name).unwrap(),
        }
    }

    fn snapshot(&self) -> rocksdb::rocksdb::Snapshot<&DB> {
        self.db.snapshot()
    }
}
