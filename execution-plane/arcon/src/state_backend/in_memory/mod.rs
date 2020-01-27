// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    collections::HashMap,
    rc::Rc,
    cell::{RefCell, Ref, RefMut},
    marker::PhantomData,
    fmt::Debug,
    io::Write
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
            aggregating_state::InMemoryAggregatingState
        }
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

    pub fn new_value_state<IK, N, T>(&self, init_item_key: IK, init_namespace: N) -> InMemoryValueState<IK, N, T> {
        let common = self.new_state_common(init_item_key, init_namespace);
        InMemoryValueState { common, _phantom: Default::default() }
    }

    pub fn new_map_state<IK, N, K, V>(&self, init_item_key: IK, init_namespace: N) -> InMemoryMapState<IK, N, K, V> {
        let common = self.new_state_common(init_item_key, init_namespace);
        InMemoryMapState { common, _phantom: Default::default() }
    }

    pub fn new_vec_state<IK, N, T>(&self, init_item_key: IK, init_namespace: N) -> InMemoryVecState<IK, N, T> {
        let common = self.new_state_common(init_item_key, init_namespace);
        InMemoryVecState { common, _phantom: Default::default() }
    }

    pub fn new_reducing_state<IK, N, T, F>(&self, init_item_key: IK, init_namespace: N, reduce_fn: F) -> InMemoryReducingState<IK, N, T, F>
        where F: Fn(&T, &T) -> T {
        let common = self.new_state_common(init_item_key, init_namespace);
        InMemoryReducingState { common, reduce_fn, _phantom: Default::default() }
    }

    pub fn new_aggregating_state<IK, N, T, AGG>(&self, init_item_key: IK, init_namespace: N, aggregator: AGG) -> InMemoryAggregatingState<IK, N, T, AGG>
        where AGG: Aggregator<T> {
        let common = self.new_state_common(init_item_key, init_namespace);
        InMemoryAggregatingState { common, aggregator, _phantom: Default::default() }
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
}

impl StateBackend for InMemory {
    fn new(_name: &str) -> ArconResult<InMemory> {
        Ok(InMemory { db: HashMap::new() })
    }

    fn get_cloned(&self, key: &[u8]) -> ArconResult<Vec<u8>> {
        if let Some(data) = self.db.get(key) {
            Ok(data.to_vec())
        } else {
            return arcon_err!("{}", "Value not found");
        }
    }

    // TODO: unnecessary copy
    fn put(&mut self, key: &[u8], value: &[u8]) -> ArconResult<()> {
        self.db.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn remove(&mut self, key: &[u8]) -> ArconResult<()> {
        let _ = self.db.remove(key);
        Ok(())
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
        let _ = db.put(key.as_bytes(), value.as_bytes()).unwrap();
        let fetched = db.get_cloned(key.as_bytes()).unwrap();
        assert_eq!(value, String::from_utf8_lossy(&fetched));
        db.remove(key.as_bytes()).unwrap();
        let res = db.get_cloned(key.as_bytes());
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
