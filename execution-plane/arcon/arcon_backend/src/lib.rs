#[cfg(test)]
extern crate tempfile;

pub mod in_memory;
#[cfg(feature = "rocksdb")]
pub mod rocksdb;

use arcon_error::ArconResult;

/// Trait required for all state backend implementations in Arcon
pub trait StateBackend: Send + Sync {
    fn create(name: &str) -> Self
    where
        Self: Sized;
    fn put(&mut self, key: &[u8], value: &[u8]) -> ArconResult<()>;
    fn get(&self, key: &[u8]) -> ArconResult<Vec<u8>>;
    fn checkpoint(&self, id: String) -> ArconResult<()>;
}
