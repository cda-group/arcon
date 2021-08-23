// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    error::ArconResult,
    index::{AppenderIndex, HashTable, IndexOps, IndexValue},
    table::ImmutableTable,
};
use arcon_state::{
    backend::{handles::ActiveHandle, Backend, VecState},
    error::*,
};
use prost::*;
use std::ops::{Deref, DerefMut};

const DEFAULT_APPENDER_SIZE: usize = 1024;

pub mod eager;

#[derive(Clone, Message)]
pub struct ProstVec<V: IndexValue> {
    #[prost(message, repeated, tag = "1")]
    data: Vec<V>,
}

impl<V> Deref for ProstVec<V>
where
    V: IndexValue,
{
    type Target = Vec<V>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<V> DerefMut for ProstVec<V>
where
    V: IndexValue,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

/// An Index suitable for Non-associative Windows
///
/// A backing [VecState] acts as an overflow vector when
/// the data no longer fits in the specified in-memory capacity.
pub struct LazyAppender<V, B>
where
    V: IndexValue,
    B: Backend,
{
    current_key: u64,
    /// A handle to the VecState
    handle: ActiveHandle<B, VecState<V>>,
    hash_table: HashTable<u64, ProstVec<V>, B>,
}

impl<V, B> LazyAppender<V, B>
where
    V: IndexValue,
    B: Backend,
{
}

impl<V, B> IndexOps for LazyAppender<V, B>
where
    V: IndexValue,
    B: Backend,
{
    fn persist(&mut self) -> ArconResult<()> {
        // for each modified, set handle key and drain
        unimplemented!();
    }
    fn set_key(&mut self, key: u64) {
        self.current_key = key;
    }
    fn table(&mut self) -> ArconResult<Option<ImmutableTable>> {
        Ok(None)
    }
}

impl<V, B> AppenderIndex<V> for LazyAppender<V, B>
where
    V: IndexValue,
    B: Backend,
{
    #[inline]
    fn append(&mut self, _: V) -> Result<()> {
        unimplemented!();
    }
    #[inline]
    fn consume(&mut self) -> Result<Vec<V>> {
        unimplemented!();
    }
    #[inline]
    fn len(&self) -> usize {
        unimplemented!();
    }
    #[inline]
    fn is_empty(&self) -> bool {
        unimplemented!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::temp_backend;
    use arcon_state::Sled;
    use eager::EagerAppender;
    use std::sync::Arc;

    fn index_test(mut index: impl AppenderIndex<u64>) -> Result<()> {
        index.set_key(0);
        for i in 0..1024 {
            index.append(i as u64)?;
        }

        assert_eq!(index.len(), 1024);
        let consumed = index.consume()?;
        assert_eq!(consumed.len(), 1024);

        for (c, i) in consumed.into_iter().enumerate() {
            assert_eq!(c as u64, i);
        }

        index.set_key(1);

        for i in 0..524 {
            index.append(i as u64)?;
        }

        assert_eq!(index.len(), 524);

        Ok(())
    }

    #[test]
    fn eager_appender_test() {
        let backend = Arc::new(temp_backend::<Sled>());
        let index = EagerAppender::new("appender", backend);
        assert!(index_test(index).is_ok());
    }
}
