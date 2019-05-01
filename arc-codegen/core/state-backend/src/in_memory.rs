use crate::error::ErrorKind::GetError;
use crate::error::*;
use crate::StateBackend;
use std::collections::HashMap;

pub struct InMemory {
    db: HashMap<Vec<u8>, Vec<u8>>,
}

impl StateBackend for InMemory {
    fn create(_name: &str) -> InMemory {
        InMemory { db: HashMap::new() }
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        if let Some(v) = self.db.get(key) {
            Ok(v.to_vec())
        } else {
            Err(Error::new(GetError("value not found".to_string())))
        }
    }

    fn checkpoint(&self, _id: String) -> Result<()> {
        panic!("InMemory backend does not support snapshots");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_mem_test() {
        let mut db = InMemory::create("test");
        let key = "key";
        let value = "hej";
        let _ = db.put(key.as_bytes(), value.as_bytes()).unwrap();
        let fetched = db.get(key.as_bytes()).unwrap();
        assert_eq!(value, String::from_utf8_lossy(&fetched));
    }
}
