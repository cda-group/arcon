// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    collections::HashMap,
    fmt::Debug,
    io::Write,
};
use uuid::Uuid;
use serde::{Serialize, Deserialize};
use arcon_error::*;
use crate::{
    state_backend::{
        state_types::*,
        StateBackend,
        in_memory::{
            value_state::InMemoryValueState,
            map_state::InMemoryMapState,
            vec_state::InMemoryVecState,
            reducing_state::InMemoryReducingState,
            aggregating_state::InMemoryAggregatingState,
        },
        ValueStateBuilder,
        MapStateBuilder,
        VecStateBuilder,
        ReducingStateBuilder,
        AggregatingStateBuilder
    }
};

pub struct InMemory {
    db: HashMap<Vec<u8>, Vec<u8>>
}

impl InMemory {
    fn new_state_common<IK, N>(&self, init_item_key: IK, init_namespace: N) -> StateCommon<IK, N> {
        StateCommon {
            id: Uuid::new_v4(),
            curr_key: init_item_key,
            curr_namespace: init_namespace,
        }
    }

    /// returns how many entries were removed
    pub fn remove_matching(&mut self, prefix: &[u8]) -> ArconResult<()> {
        let db = &mut self.db;
        db.retain(|k, _| &k[..prefix.len()] != prefix);

        Ok(())
    }

    pub fn iter_matching(&self, prefix: impl AsRef<[u8]> + Debug) -> impl Iterator<Item=(&[u8], &[u8])> {
        self.db.iter().filter_map(move |(k, v)| {
            if &k[..prefix.as_ref().len()] != prefix.as_ref() { return None; }
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
        Ok(self.db.entry(key.to_vec()).or_insert(vec![]))
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

impl<IK, N, T> ValueStateBuilder<IK, N, T> for InMemory
    where IK: Serialize, N: Serialize, T: Serialize + for<'a> Deserialize<'a> {
    type Type = InMemoryValueState<IK, N, T>;

    fn new_value_state(&self, init_item_key: IK, init_namespace: N) -> Self::Type {
        let common = self.new_state_common(init_item_key, init_namespace);
        InMemoryValueState { common, _phantom: Default::default() }
    }
}

impl<IK, N, K, V> MapStateBuilder<IK, N, K, V> for InMemory
    where IK: Serialize + for<'a> Deserialize<'a>, N: Serialize + for<'a> Deserialize<'a>,
          K: Serialize + for<'a> Deserialize<'a>, V: Serialize + for<'a> Deserialize<'a> {
    type Type = InMemoryMapState<IK, N, K, V>;

    fn new_map_state(&self, init_item_key: IK, init_namespace: N) -> Self::Type {
        let common = self.new_state_common(init_item_key, init_namespace);
        InMemoryMapState { common, _phantom: Default::default() }
    }
}

impl<IK, N, T> VecStateBuilder<IK, N, T> for InMemory
    where IK: Serialize, N: Serialize, T: Serialize + for<'a> Deserialize<'a> {
    type Type = InMemoryVecState<IK, N, T>;

    fn new_vec_state(&self, init_item_key: IK, init_namespace: N) -> Self::Type {
        let common = self.new_state_common(init_item_key, init_namespace);
        InMemoryVecState { common, _phantom: Default::default() }
    }
}

impl<IK, N, T, F> ReducingStateBuilder<IK, N, T, F> for InMemory
    where IK: Serialize, N: Serialize, T: Serialize + for<'a> Deserialize<'a>, F: Fn(&T, &T) -> T {
    type Type = InMemoryReducingState<IK, N, T, F>;

    fn new_reducing_state(&self, init_item_key: IK, init_namespace: N, reduce_fn: F) -> Self::Type {
        let common = self.new_state_common(init_item_key, init_namespace);
        InMemoryReducingState { common, reduce_fn, _phantom: Default::default() }
    }
}


impl<IK, N, T, AGG> AggregatingStateBuilder<IK, N, T, AGG> for InMemory
    where IK: Serialize, N: Serialize, AGG: Aggregator<T>, AGG::Accumulator: Serialize + for<'a> Deserialize<'a> {
    type Type = InMemoryAggregatingState<IK, N, T, AGG>;

    fn new_aggregating_state(&self, init_item_key: IK, init_namespace: N, aggregator: AGG) -> Self::Type {
        let common = self.new_state_common(init_item_key, init_namespace);
        InMemoryAggregatingState { common, aggregator, _phantom: Default::default() }
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
    curr_key: IK,
    curr_namespace: N,
}

fn serialize_keys_and_namespace_into<IK, N, UK>(item_key: &IK, namespace: &N, user_key: &UK, writer: &mut impl Write) -> ArconResult<()>
    where IK: Serialize, N: Serialize, UK: Serialize {
    bincode::serialize_into(writer, &(
        item_key,
        namespace,
        user_key
    )).map_err(|e| arcon_err_kind!("Could not serialize keys and namespace: {}", e))
}

impl<IK, N> StateCommon<IK, N> where IK: Serialize, N: Serialize {
    fn get_db_key<UK>(&self, user_key: &UK) -> ArconResult<Vec<u8>> where UK: Serialize {
        let mut res = self.id.as_bytes().to_vec();
        serialize_keys_and_namespace_into(&self.curr_key, &self.curr_namespace, user_key, &mut res)?;

        Ok(res)
    }
}

macro_rules! delegate_key_and_namespace {
    ($common: ident) => {
        fn get_current_key(&self) -> ArconResult<&IK> {
            Ok(&self.$common.curr_key)
        }

        fn set_current_key(&mut self, new_key: IK) -> ArconResult<()> {
            self.$common.curr_key = new_key;
            Ok(())
        }

        fn get_current_namespace(&self) -> ArconResult<&N> {
            Ok(&self.$common.curr_namespace)
        }

        fn set_current_namespace(&mut self, new_namespace: N) -> ArconResult<()> {
            self.$common.curr_namespace = new_namespace;
            Ok(())
        }
    };
}

mod value_state;
mod map_state;
mod vec_state;
mod reducing_state;
mod aggregating_state;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_mem_test() {
        let mut db = InMemory::new("test").unwrap();
        let key = "key";
        let value = "hej";
        let _ = db.put(key.to_string().into_bytes(), value.to_string().into_bytes()).unwrap();
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

        let mut v = vec![];
        serialize_keys_and_namespace_into(&42, &255, &(), &mut v);
        let v = dbg!(v);

        let mut v2 = vec![];
        serialize_keys_and_namespace_into(&42, &255, &"hello", &mut v2);
        let v2 = dbg!(v2);

        assert_eq!(&v2[..v.len()], &v[..]);
    }
}
