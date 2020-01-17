// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::state_backend::StateBackend;
use arcon_error::*;
use std::collections::HashMap;

pub struct InMemory {
    db: HashMap<Vec<u8>, Vec<u8>>,
}

impl StateBackend for InMemory {
    fn create(_name: &str) -> ArconResult<InMemory> {
        Ok(InMemory { db: HashMap::new() })
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> ArconResult<()> {
        self.db.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn get(&self, key: &[u8]) -> ArconResult<Vec<u8>> {
        if let Some(data) = self.db.get(key) {
            Ok(data.to_vec())
        } else {
            return arcon_err!("{}", "Value not found");
        }
    }

    fn checkpoint(&self, _id: String) -> ArconResult<()> {
        panic!("InMemory backend does not support snapshots");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_mem_test() {
        let mut db = InMemory::create("test").unwrap();
        let key = "key";
        let value = "hej";
        let _ = db.put(key.as_bytes(), value.as_bytes()).unwrap();
        let fetched = db.get(key.as_bytes()).unwrap();
        assert_eq!(value, String::from_utf8_lossy(&fetched));
    }
}
