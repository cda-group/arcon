// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::state_backend::{
    in_memory::{
        aggregating_state::InMemoryAggregatingState, map_state::InMemoryMapState,
        reducing_state::InMemoryReducingState, value_state::InMemoryValueState,
        vec_state::InMemoryVecState,
    },
    state_types::*,
    AggregatingStateBuilder, MapStateBuilder, ReducingStateBuilder, StateBackend,
    ValueStateBuilder, VecStateBuilder,
};
use arcon_error::*;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Debug, io::Write};
use uuid::Uuid;

pub struct InMemory {
    db: HashMap<Vec<u8>, Vec<u8>>,
}

impl InMemory {
    // we don't care about the state name, since it cannot be snapshotted
    fn new_state_common<IK, N>(&self, init_item_key: IK, init_namespace: N) -> StateCommon<IK, N> {
        StateCommon {
            id: Uuid::new_v4(),
            item_key: init_item_key,
            namespace: init_namespace,
        }
    }

    /// returns how many entries were removed
    pub fn remove_matching(&mut self, prefix: &[u8]) -> ArconResult<()> {
        let db = &mut self.db;
        db.retain(|k, _| &k[..prefix.len()] != prefix);

        Ok(())
    }

    pub fn iter_matching(
        &self,
        prefix: impl AsRef<[u8]> + Debug,
    ) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.db.iter().filter_map(move |(k, v)| {
            if &k[..prefix.as_ref().len()] != prefix.as_ref() {
                return None;
            }
            Some((k.as_slice(), v.as_slice()))
        })
    }

    pub fn contains(&self, key: &[u8]) -> ArconResult<bool> {
        Ok(self.db.contains_key(key))
    }

    pub fn get(&self, key: &[u8]) -> ArconResult<&[u8]> {
        if let Some(data) = self.db.get(key) {
            Ok(&*data)
        } else {
            return arcon_err!("Value not found");
        }
    }

    pub fn get_mut(&mut self, key: &[u8]) -> ArconResult<&mut Vec<u8>> {
        if let Some(data) = self.db.get_mut(key) {
            Ok(data)
        } else {
            return arcon_err!("Value not found");
        }
    }

    pub fn get_mut_or_init_empty(&mut self, key: &[u8]) -> ArconResult<&mut Vec<u8>> {
        Ok(self.db.entry(key.to_vec()).or_insert_with(|| vec![]))
    }

    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> ArconResult<()> {
        self.db.insert(key, value);
        Ok(())
    }

    fn remove(&mut self, key: &[u8]) -> ArconResult<()> {
        let _ = self.db.remove(key);
        Ok(())
    }
}

// since we don't do checkpointing for InMemory state backend, the name of the state is simply discarded
// TODO: maybe keep it for debugging purposes?
impl<IK, N, T> ValueStateBuilder<IK, N, T> for InMemory
where
    IK: Serialize,
    N: Serialize,
    T: Serialize + for<'a> Deserialize<'a>,
{
    type Type = InMemoryValueState<IK, N, T>;

    fn new_value_state(&mut self, _name: &str, init_item_key: IK, init_namespace: N) -> Self::Type {
        let common = self.new_state_common(init_item_key, init_namespace);
        InMemoryValueState {
            common,
            _phantom: Default::default(),
        }
    }
}

impl<IK, N, K, V> MapStateBuilder<IK, N, K, V> for InMemory
where
    IK: Serialize + for<'a> Deserialize<'a>,
    N: Serialize + for<'a> Deserialize<'a>,
    K: Serialize + for<'a> Deserialize<'a>,
    V: Serialize + for<'a> Deserialize<'a>,
{
    type Type = InMemoryMapState<IK, N, K, V>;

    fn new_map_state(&mut self, _name: &str, init_item_key: IK, init_namespace: N) -> Self::Type {
        let common = self.new_state_common(init_item_key, init_namespace);
        InMemoryMapState {
            common,
            _phantom: Default::default(),
        }
    }
}

impl<IK, N, T> VecStateBuilder<IK, N, T> for InMemory
where
    IK: Serialize,
    N: Serialize,
    T: Serialize + for<'a> Deserialize<'a>,
{
    type Type = InMemoryVecState<IK, N, T>;

    fn new_vec_state(&mut self, _name: &str, init_item_key: IK, init_namespace: N) -> Self::Type {
        let common = self.new_state_common(init_item_key, init_namespace);
        InMemoryVecState {
            common,
            _phantom: Default::default(),
        }
    }
}

impl<IK, N, T, F> ReducingStateBuilder<IK, N, T, F> for InMemory
where
    IK: Serialize,
    N: Serialize,
    T: Serialize + for<'a> Deserialize<'a>,
    F: Fn(&T, &T) -> T,
{
    type Type = InMemoryReducingState<IK, N, T, F>;

    fn new_reducing_state(
        &mut self,
        _name: &str,
        init_item_key: IK,
        init_namespace: N,
        reduce_fn: F,
    ) -> Self::Type {
        let common = self.new_state_common(init_item_key, init_namespace);
        InMemoryReducingState {
            common,
            reduce_fn,
            _phantom: Default::default(),
        }
    }
}

impl<IK, N, T, AGG> AggregatingStateBuilder<IK, N, T, AGG> for InMemory
where
    IK: Serialize,
    N: Serialize,
    AGG: Aggregator<T>,
    AGG::Accumulator: Serialize + for<'a> Deserialize<'a>,
{
    type Type = InMemoryAggregatingState<IK, N, T, AGG>;

    fn new_aggregating_state(
        &mut self,
        _name: &str,
        init_item_key: IK,
        init_namespace: N,
        aggregator: AGG,
    ) -> Self::Type {
        let common = self.new_state_common(init_item_key, init_namespace);
        InMemoryAggregatingState {
            common,
            aggregator,
            _phantom: Default::default(),
        }
    }
}

impl StateBackend for InMemory {
    fn new(_name: &str) -> ArconResult<InMemory> {
        Ok(InMemory { db: HashMap::new() })
    }

    fn checkpoint(&self, _id: String) -> ArconResult<()> {
        arcon_err!("InMemory backend snapshotting is not implemented")
    }
}

pub(crate) struct StateCommon<IK, N> {
    id: Uuid,
    item_key: IK,
    namespace: N,
}

impl<IK, N> StateCommon<IK, N>
where
    IK: Serialize,
    N: Serialize,
{
    fn get_db_key<UK>(&self, user_key: &UK) -> ArconResult<Vec<u8>>
    where
        UK: Serialize,
    {
        // UUID is not serializable TODO: there's probably a feature flag for this
        let mut res = self.id.as_bytes().to_vec();
        bincode::serialize_into(&mut res, &(&self.item_key, &self.namespace, user_key))
            .map_err(|e| arcon_err_kind!("Could not serialize keys and namespace: {}", e))?;

        Ok(res)
    }
}

mod aggregating_state;
mod map_state;
mod reducing_state;
mod value_state;
mod vec_state;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_mem_test() {
        let mut db = InMemory::new("test").unwrap();
        let key = "key";
        let value = "hej";
        db.put(key.to_string().into_bytes(), value.to_string().into_bytes())
            .unwrap();
        let fetched = db.get(key.as_bytes()).unwrap();
        assert_eq!(value, String::from_utf8_lossy(fetched));
        db.remove(key.as_bytes()).unwrap();
        let res = db.get(key.as_bytes());
        assert!(res.is_err());
    }

    #[test]
    fn test_namespace_serialization() {
        // we rely on the order of the serialized fields, because we search by prefix when clearing
        // map state

        let state = StateCommon {
            id: Uuid::new_v4(),
            item_key: 42,
            namespace: 255,
        };

        let v = state.get_db_key(&()).unwrap();
        let v2 = state.get_db_key(&"hello").unwrap();

        assert_eq!(&v2[..v.len()], &v[..]);
    }
}
