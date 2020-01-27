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

    /// returns how many entries were removed
    pub fn remove_matching(&mut self, prefix: &[u8]) -> ArconResult<usize> {
        let db = &mut self.db;
        let mut total = 0;

        db.retain(|k, _| {
            if &k[..prefix.len()] == prefix {
                total += 1;
                false
            } else {
                true
            }
        });

        Ok(total)
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

struct StateCommon<IK, N> {
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

pub struct InMemoryValueState<IK, N, T> {
    common: StateCommon<IK, N>,
    _phantom: PhantomData<T>,
}

impl<IK, N, T> State<InMemory, IK, N> for InMemoryValueState<IK, N, T>
    where IK: Serialize, N: Serialize, T: Serialize {
    fn clear(&self, backend: &mut InMemory) -> ArconResult<()> {
        let key = self.common.get_db_key(&())?;
        backend.remove(&key)?;
        Ok(())
    }

    delegate_key_and_namespace!(common);
}

impl<IK, N, T> ValueState<InMemory, IK, N, T> for InMemoryValueState<IK, N, T>
    where IK: Serialize, N: Serialize, T: Serialize, T: for<'a> Deserialize<'a> {
    fn get(&self, backend: &InMemory) -> ArconResult<T> {
        let key = self.common.get_db_key(&())?;
        let serialized = backend.get(&key)?;
        let value = bincode::deserialize(&serialized)
            .map_err(|e| arcon_err_kind!("Cannot deserialize value state: {}", e))?;
        Ok(value)
    }

    fn set(&self, backend: &mut InMemory, new_value: T) -> ArconResult<()> {
        let key = self.common.get_db_key(&())?;
        let serialized = bincode::serialize(&new_value)
            .map_err(|e| arcon_err_kind!("Cannot serialize value state: {}", e))?;
        backend.put(&key, &serialized)?;
        Ok(())
    }
}

pub struct InMemoryMapState<IK, N, K, V> {
    common: StateCommon<IK, N>,
    _phantom: PhantomData<(K, V)>,
}

impl<IK, N, K, V> State<InMemory, IK, N> for InMemoryMapState<IK, N, K, V>
    where IK: Serialize, N: Serialize {
    fn clear(&self, backend: &mut InMemory) -> ArconResult<()> {
        // () is not serialized, and the user key is the tail of the db key, so in effect we get
        // the prefix with which to search the underlying db.
        let prefix = self.common.get_db_key(&())?;
        backend.remove_matching(&prefix)?;

        Ok(())
    }


    delegate_key_and_namespace!(common);
}

impl<IK, N, K, V> MapState<InMemory, IK, N, K, V> for InMemoryMapState<IK, N, K, V>
    where
        IK: Serialize + for<'a> Deserialize<'a>,
        N: Serialize + for<'a> Deserialize<'a>,
        K: Serialize + for<'a> Deserialize<'a>,
        V: Serialize + for<'a> Deserialize<'a> {
    fn get(&self, backend: &InMemory, key: &K) -> ArconResult<V> {
        let key = self.common.get_db_key(key)?;
        let serialized = backend.get(&key)?;
        let value = bincode::deserialize(&serialized)
            .map_err(|e| arcon_err_kind!("Cannot deserialize map state value: {}", e))?;

        Ok(value)
    }

    fn put(&self, backend: &mut InMemory, key: K, value: V) -> ArconResult<()> {
        let key = self.common.get_db_key(&key)?;
        let serialized = bincode::serialize(&value)
            .map_err(|e| arcon_err_kind!("Could not serialize map state value: {}", e))?;
        backend.put(&key, &serialized)?;

        Ok(())
    }

    fn put_all_dyn(&self, backend: &mut InMemory, key_value_pairs: &mut dyn Iterator<Item=(K, V)>) -> ArconResult<()> {
        self.put_all(backend, key_value_pairs)
    }

    fn put_all(&self, backend: &mut InMemory, key_value_pairs: impl IntoIterator<Item=(K, V)>) -> ArconResult<()> where Self: Sized {
        for (k, v) in key_value_pairs.into_iter() {
            self.put(backend, k, v)?; // TODO: what if one fails? partial insert? should we rollback?
        }

        Ok(())
    }

    fn remove(&self, backend: &mut InMemory, key: &K) -> ArconResult<()> {
        let key = self.common.get_db_key(key)?;
        backend.remove(&key)?;

        Ok(())
    }

    fn contains(&self, backend: &InMemory, key: &K) -> ArconResult<bool> {
        let key = self.common.get_db_key(key)?;
        backend.contains(&key)
    }

    // TODO: unboxed versions of below
    fn iter<'a>(&self, backend: &'a InMemory) -> ArconResult<Box<dyn Iterator<Item=(K, V)> + 'a>> {
        let prefix = self.common.get_db_key(&())?;
        let id_len = self.common.id.as_bytes().len();
        let iter = backend.iter_matching(prefix).map(move |(k, v)| {
            let (_, _, key): (IK, N, K) = bincode::deserialize(&k[id_len..])
                .map_err(|e| arcon_err_kind!("Could not deserialize map state key: {}", e))?;
            let value: V = bincode::deserialize(v)
                .map_err(|e| arcon_err_kind!("Could not deserialize map state value: {}", e))?;
            Ok((key, value))
        }).map(|res: ArconResult<(K, V)>| res.expect("deserialization error"));
        // TODO: we panic above if deserialization fails. Perhaps the function signature should
        //  change to accommodate for that

        Ok(Box::new(iter))
    }

    fn keys<'a>(&self, backend: &'a InMemory) -> ArconResult<Box<dyn Iterator<Item=K> + 'a>> {
        let prefix = self.common.get_db_key(&())?;
        let id_len = self.common.id.as_bytes().len();
        let iter = backend.iter_matching(prefix).map(move |(k, _)| {
            let (_, _, key): (IK, N, K) = bincode::deserialize(&k[id_len..])
                .map_err(|e| arcon_err_kind!("Could not deserialize map state key: {:?}", e))?;
            Ok(key)
        }).map(|res: ArconResult<K>| res.expect("deserialization error"));

        Ok(Box::new(iter))
    }

    fn values<'a>(&self, backend: &'a InMemory) -> ArconResult<Box<dyn Iterator<Item=V> + 'a>> {
        let prefix = self.common.get_db_key(&())?;
        let iter = backend.iter_matching(prefix).map(|(_, v)| {
            let value: V = bincode::deserialize(v)
                .map_err(|e| arcon_err_kind!("Could not deserialize map state value: {}", e))?;
            Ok(value)
        }).map(|res: ArconResult<V>| res.expect("deserialization error"));

        Ok(Box::new(iter))
    }

    fn is_empty(&self, backend: &InMemory) -> ArconResult<bool> {
        let prefix = self.common.get_db_key(&())?;
        Ok(backend.iter_matching(prefix).next().is_none())
    }
}

pub struct InMemoryVecState<IK, N, T> {
    common: StateCommon<IK, N>,
    _phantom: PhantomData<T>,
}

impl<IK, N, T> State<InMemory, IK, N> for InMemoryVecState<IK, N, T>
    where IK: Serialize, N: Serialize {
    fn clear(&self, backend: &mut InMemory) -> ArconResult<()> {
        let key = self.common.get_db_key(&())?;
        backend.remove(&key)
    }

    delegate_key_and_namespace!(common);
}

impl<IK, N, T> AppendingState<InMemory, IK, N, T, Vec<T>> for InMemoryVecState<IK, N, T>
    where IK: Serialize, N: Serialize, T: Serialize, T: for<'a> Deserialize<'a> {
    fn get(&self, backend: &InMemory) -> ArconResult<Vec<T>> {
        let key = self.common.get_db_key(&())?;
        let serialized = backend.get(&key)?;

        // reader is updated in the loop to point at the yet unconsumed part of the serialized data
        let mut reader = &serialized[..];
        let mut res = vec![];
        while !reader.is_empty() {
            let val = bincode::deserialize_from(&mut reader)
                .map_err(|e| arcon_err_kind!("Could not deserialize vec state value: {}", e))?;
            res.push(val);
        }

        Ok(res)
    }

    fn append(&self, backend: &mut InMemory, value: T) -> ArconResult<()> {
        let key = self.common.get_db_key(&())?;
        let storage = backend.get_mut_or_init_empty(&key)?;

        bincode::serialize_into(storage, &value)
            .map_err(|e| arcon_err_kind!("Could not serialize vec state value: {}", e))
    }
}

impl<IK, N, T> MergingState<InMemory, IK, N, T, Vec<T>> for InMemoryVecState<IK, N, T>
    where IK: Serialize, N: Serialize, T: Serialize, T: for<'a> Deserialize<'a> {}

impl<IK, N, T> VecState<InMemory, IK, N, T> for InMemoryVecState<IK, N, T>
    where IK: Serialize, N: Serialize, T: Serialize, T: for<'a> Deserialize<'a> {
    fn set(&self, backend: &mut InMemory, value: Vec<T>) -> ArconResult<()> {
        let key = self.common.get_db_key(&())?;
        let mut storage = vec![];
        for elem in value {
            bincode::serialize_into(&mut storage, &elem)
                .map_err(|e| arcon_err_kind!("Could not serialize vec state value: {}", e))?;
        }
        backend.put(&key, &storage)
    }

    fn add_all(&self, backend: &mut InMemory, values: impl IntoIterator<Item=T>) -> ArconResult<()> where Self: Sized {
        let key = self.common.get_db_key(&())?;
        let mut storage = backend.get_mut_or_init_empty(&key)?;

        for value in values {
            bincode::serialize_into(&mut storage, &value)
                .map_err(|e| arcon_err_kind!("Could not serialize vec state value: {}", e))?;
        }

        Ok(())
    }

    fn add_all_dyn(&self, backend: &mut InMemory, values: &mut dyn Iterator<Item=T>) -> ArconResult<()> {
        self.add_all(backend, values)
    }

    fn len(&self, backend: &InMemory) -> ArconResult<usize> {
        let key = self.common.get_db_key(&())?;
        let storage = backend.get(&key);

        match storage {
            Err(e) => {
                match e.kind() {
                    ErrorKind::ArconError(message) if &*message == "Value not found" => Ok(0),
                    _ => Err(e)
                }
            }
            Ok(buf) => {
                let mut reader = buf;
                if buf.len() == 0 { return Ok(0); }

                // it's a bit unfortunate, but we need a value of a given type T, to compute its
                // bincode serialized size
                let first_value: T = bincode::deserialize_from(&mut reader)
                    .map_err(|e| arcon_err_kind!("Could not deserialize vec state value: {}", e))?;
                let first_value_serialized_size = bincode::serialized_size(&first_value)
                    .map_err(|e| arcon_err_kind!("Could not get the size of serialized vec state value: {}", e))? as usize;

                debug_assert_ne!(first_value_serialized_size, 0);

                let len = buf.len() / first_value_serialized_size;
                let rem = buf.len() % first_value_serialized_size;

                // sanity check
                if rem != 0 { return arcon_err!("vec state storage length is not a multiple of element size"); }

                Ok(len)
            }
        }
    }
}

pub struct InMemoryReducingState<IK, N, T, F> {
    common: StateCommon<IK, N>,
    reduce_fn: F,
    _phantom: PhantomData<T>,
}

impl<IK, N, T, F> State<InMemory, IK, N> for InMemoryReducingState<IK, N, T, F>
    where IK: Serialize, N: Serialize {
    fn clear(&self, backend: &mut InMemory) -> ArconResult<()> {
        let key = self.common.get_db_key(&())?;
        backend.remove(&key)?;
        Ok(())
    }

    delegate_key_and_namespace!(common);
}

impl<IK, N, T, F> AppendingState<InMemory, IK, N, T, T> for InMemoryReducingState<IK, N, T, F>
// TODO: if we made the (backend-)mutating methods take &mut self, F could be FnMut
    where IK: Serialize, N: Serialize, T: Serialize + for<'a> Deserialize<'a>, F: Fn(&T, &T) -> T {
    fn get(&self, backend: &InMemory) -> ArconResult<T> {
        let key = self.common.get_db_key(&())?;
        let storage = backend.get(&key)?;
        let value = bincode::deserialize(storage)
            .map_err(|e| arcon_err_kind!("Could not deserialize reducing state value: {}", e))?;

        Ok(value)
    }

    fn append(&self, backend: &mut InMemory, value: T) -> ArconResult<()> {
        let key = self.common.get_db_key(&())?;
        let storage = backend.get_mut_or_init_empty(&key)?;
        if storage.is_empty() {
            bincode::serialize_into(storage, &value)
                .map_err(|e| arcon_err_kind!("Could not serialize reducing state value: {}", e))?;
            return Ok(())
        }

        let old_value = bincode::deserialize(storage)
            .map_err(|e| arcon_err_kind!("Could not deserialize reducing state value: {}", e))?;

        let new_value = (self.reduce_fn)(&old_value, &value);

        let key = self.common.get_db_key(&())?;
        bincode::serialize_into(storage.as_mut_slice(), &new_value)
            .map_err(|e| arcon_err_kind!("Could not serialize reducing state value: {}", e))?;

        Ok(())
    }
}

impl<IK, N, T, F> MergingState<InMemory, IK, N, T, T> for InMemoryReducingState<IK, N, T, F>
// TODO: if we made the (backend-)mutating methods take &mut self, F could be FnMut
    where IK: Serialize, N: Serialize, T: Serialize + for<'a> Deserialize<'a>, F: Fn(&T, &T) -> T {}

impl<IK, N, T, F> ReducingState<InMemory, IK, N, T> for InMemoryReducingState<IK, N, T, F>
    where IK: Serialize, N: Serialize, T: Serialize + for<'a> Deserialize<'a>, F: Fn(&T, &T) -> T {}

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
        let mut reducing_state = db.new_reducing_state::<_, _, i32, _>((), (),
                                                                       |old: &i32, new: &i32| *old.max(new));

        reducing_state.append(&mut db, 7).unwrap();
        reducing_state.append(&mut db, 42).unwrap();
        reducing_state.append(&mut db, 10).unwrap();

        assert_eq!(reducing_state.get(&db).unwrap(), 42);
    }
}
