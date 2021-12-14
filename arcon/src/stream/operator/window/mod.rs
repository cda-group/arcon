pub mod assigner;

pub use assigner::{WindowAssigner, WindowState};

use fxhash::FxHasher;
use std::hash::{Hash, Hasher};

#[derive(prost::Message, Hash, PartialEq, Eq, Copy, Clone)]
pub struct WindowContext {
    #[prost(uint64)]
    pub key: u64,
    #[prost(uint64)]
    pub index: u64,
}

impl WindowContext {
    pub fn new(key: u64, index: u64) -> Self {
        WindowContext { key, index }
    }
}

impl From<WindowContext> for u64 {
    fn from(ctx: WindowContext) -> Self {
        let mut s = FxHasher::default();
        ctx.hash(&mut s);
        s.finish()
    }
}
