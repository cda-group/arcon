// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::state_backend::StateBackend;
use crate::state_backend::state_types::*;
use arcon_error::*;
use std::collections::HashMap;
use std::rc::Rc;
use std::cell::{RefCell, Ref, RefMut};
use std::marker::PhantomData;
use std::fmt::Debug;
use std::io::Write;
use uuid::Uuid;
use serde::{Serialize, Deserialize};
use crate::state_backend::in_memory::value_state::InMemoryValueState;
use crate::state_backend::in_memory::map_state::InMemoryMapState;
use crate::state_backend::in_memory::vec_state::InMemoryVecState;
use crate::state_backend::in_memory::reducing_state::InMemoryReducingState;
use crate::state_backend::in_memory::aggregating_state::InMemoryAggregatingState;

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
    fn in_memory_value_state_test() {
        let mut db = InMemory::new("test").unwrap();
        let mut value_state = db.new_value_state((), ());

        let unset = value_state.get(&db);
        assert!(unset.is_err());

        value_state.set(&mut db, 123).unwrap();
        let set = value_state.get(&db).unwrap();
        assert_eq!(set, 123);

        value_state.clear(&mut db).unwrap();
        let cleared = value_state.get(&db);
        assert!(cleared.is_err());
    }

    #[test]
    fn in_memory_value_states_are_independant() {
        let mut db = InMemory::new("test").unwrap();
        let mut v1 = db.new_value_state((), ());
        let mut v2 = db.new_value_state((), ());

        v1.set(&mut db, 123).unwrap();
        v2.set(&mut db, 456).unwrap();

        let v1v = v1.get(&db).unwrap();
        let v2v = v2.get(&db).unwrap();
        assert_eq!(v1v, 123);
        assert_eq!(v2v, 456);

        v1.clear(&mut db).unwrap();
        let v1res = v1.get(&db);
        let v2v = v2.get(&db).unwrap();
        assert!(v1res.is_err());
        assert_eq!(v2v, 456);
    }

    #[test]
    fn in_memory_value_states_handle_state_for_different_keys_and_namespaces() {
        let mut db = InMemory::new("test").unwrap();
        let mut value_state = db.new_value_state(0, 0);

        value_state.set(&mut db, 0).unwrap();
        value_state.set_current_key(1).unwrap();
        let should_be_err = value_state.get(&db);
        assert!(should_be_err.is_err());

        value_state.set(&mut db, 1);
        let should_be_one = value_state.get(&db).unwrap();
        assert_eq!(should_be_one, 1);

        value_state.set_current_key(0).unwrap();
        let should_be_zero = value_state.get(&db).unwrap();
        assert_eq!(should_be_zero, 0);

        value_state.set_current_namespace(1).unwrap();
        let should_be_err = value_state.get(&db);
        assert!(should_be_err.is_err());

        value_state.set(&mut db, 2).unwrap();
        let should_be_two = value_state.get(&db).unwrap();
        assert_eq!(should_be_two, 2);

        value_state.set_current_namespace(0).unwrap();
        let should_be_zero = value_state.get(&db).unwrap();
        assert_eq!(should_be_zero, 0);
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

    // TODO: comprehensive tests for map, vec, reducing state, and aggregating state impls

    #[test]
    fn map_state_test() {
        let mut db = InMemory::new("test").unwrap();
        let mut map_state = db.new_map_state((), ());

        // TODO: &String is weird, maybe look at how it's done with the keys in std hash-map
        assert!(!map_state.contains(&db, &"first key".to_string()).unwrap());

        map_state.put(&mut db, "first key".to_string(), 42).unwrap();
        map_state.put(&mut db, "second key".to_string(), 69).unwrap();

        assert!(map_state.contains(&db, &"first key".to_string()).unwrap());
        assert!(map_state.contains(&db, &"second key".to_string()).unwrap());

        assert_eq!(map_state.get(&db, &"first key".to_string()).unwrap(), 42);
        assert_eq!(map_state.get(&db, &"second key".to_string()).unwrap(), 69);

        let keys: Vec<_> = map_state.keys(&db).unwrap().collect();

        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"first key".to_string()));
        assert!(keys.contains(&"second key".to_string()));
    }

    #[test]
    fn vec_state_test() {
        let mut db = InMemory::new("test").unwrap();
        let mut vec_state = db.new_vec_state((), ());
        assert_eq!(vec_state.len(&db).unwrap(), 0);

        vec_state.append(&mut db, 1).unwrap();
        vec_state.append(&mut db, 2).unwrap();
        vec_state.append(&mut db, 3).unwrap();
        vec_state.add_all(&mut db, vec![4, 5, 6]).unwrap();

        assert_eq!(vec_state.get(&db).unwrap(), vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(vec_state.len(&db).unwrap(), 6);
    }

    #[test]
    fn reducing_state_test() {
        let mut db = InMemory::new("test").unwrap();
        let mut reducing_state = db.new_reducing_state((), (),
                                                       |old: &i32, new: &i32| *old.max(new));

        reducing_state.append(&mut db, 7).unwrap();
        reducing_state.append(&mut db, 42).unwrap();
        reducing_state.append(&mut db, 10).unwrap();

        assert_eq!(reducing_state.get(&db).unwrap(), 42);
    }

    #[test]
    fn aggregating_state_test() {
        let mut db = InMemory::new("test").unwrap();
        let mut aggregating_state = db.new_aggregating_state(
            (),
            (),
            ClosuresAggregator::new(|| vec![], Vec::push, |v| format!("{:?}", v)),
        );

        aggregating_state.append(&mut db, 1).unwrap();
        aggregating_state.append(&mut db, 2).unwrap();
        aggregating_state.append(&mut db, 3).unwrap();

        assert_eq!(aggregating_state.get(&db).unwrap(), "[1, 2, 3]".to_string());
    }
}
