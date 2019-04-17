use crate::error::*;

pub trait StateBackend {
    fn create(name: &str) -> Self;
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    fn get(&self, key: &[u8]) -> Result<Vec<u8>>;
}
