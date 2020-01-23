// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::state_backend::StateBackend;
use crate::state_backend::state_types::*;
use arcon_error::*;
use std::collections::HashMap;
use std::rc::Rc;
use std::cell::RefCell;
use std::marker::PhantomData;
use uuid::Uuid;
use serde::{Serialize, Deserialize};
use std::io::Write;

#[derive(Clone)]
pub struct InMemory {
    inner: Rc<RefCell<Inner>>
}

struct Inner {
    db: HashMap<Vec<u8>, Vec<u8>>
}

impl InMemory {
    fn new_state_common<IK, N>(&self, init_item_key: IK, init_namespace: N) -> StateCommon<IK, N> {
        StateCommon {
            backend_ref: self.clone(),
            id: Uuid::new_v4(),
            curr_key: init_item_key,
            curr_namespace: init_namespace,
        }
    }

    pub fn new_value_state<IK, N, T>(&self, init_item_key: IK, init_namespace: N) -> InMemoryValueState<IK, N, T> {
        let common = self.new_state_common(init_item_key, init_namespace);
        InMemoryValueState { common, _phantom: Default::default() }
    }
}

impl StateBackend for InMemory {
    fn create_shared(_name: &str) -> ArconResult<InMemory> {
        Ok(InMemory { inner: Rc::new(RefCell::new(Inner { db: HashMap::new() })) })
    }

    fn get(&self, key: &[u8]) -> ArconResult<Vec<u8>> {
        if let Some(data) = self.inner.borrow().db.get(key) {
            Ok(data.to_vec())
        } else {
            return arcon_err!("{}", "Value not found");
        }
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> ArconResult<()> {
        self.inner.borrow_mut().db.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn remove(&mut self, key: &[u8]) -> ArconResult<()> {
        let _ = self.inner.borrow_mut().db.remove(key);
        Ok(())
    }

    fn checkpoint(&self, _id: String) -> ArconResult<()> {
        arcon_err!("InMemory backend snapshotting is not implemented")
    }

    fn new_value_state_boxed<IK: 'static, N: 'static, T: 'static>(&self, init_item_key: IK, init_namespace: N) -> Box<dyn ValueState<IK, N, T>>
        where IK: Serialize, N: Serialize, T: Serialize, T: for<'a> Deserialize<'a> {
        Box::new(self.new_value_state(init_item_key, init_namespace))
    }

    fn new_map_state_boxed<IK, N, K, V>(&self, init_item_key: IK, init_namespace: N) -> Box<dyn MapState<IK, N, K, V>> {
        unimplemented!()
    }

    fn new_vec_state_boxed<IK, N, T>(&self, init_item_key: IK, init_namespace: N) -> Box<dyn VecState<IK, N, T>> {
        unimplemented!()
    }

    fn new_reducing_state_boxed<IK, N, T>(&self, init_item_key: IK, init_namespace: N) -> Box<dyn ReducingState<IK, N, T>> {
        unimplemented!()
    }

    fn new_aggregating_state_boxed<IK, N, IN, OUT>(&self, init_item_key: IK, init_namespace: N) -> Box<dyn AggregatingState<IK, N, IN, OUT>> {
        unimplemented!()
    }
}

struct StateCommon<IK, N> {
    backend_ref: InMemory,
    id: Uuid,
    curr_key: IK,
    curr_namespace: N,
}

fn serialize_keys_and_namespace_to<IK, N, UK>(item_key: &IK, namespace: &N, user_key: &UK, writer: &mut impl Write) -> ArconResult<()>
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
        serialize_keys_and_namespace_to(&self.curr_key, &self.curr_namespace, user_key, &mut res)?;

        Ok(res)
    }
}

pub struct InMemoryValueState<IK, N, T> {
    common: StateCommon<IK, N>,
    _phantom: PhantomData<T>,
}

impl<IK, N, T> State<IK, N> for InMemoryValueState<IK, N, T>
    where IK: Serialize, N: Serialize, T: Serialize {
    fn clear(&mut self) -> ArconResult<()> {
        let key = self.common.get_db_key(&())?;
        self.common.backend_ref.remove(&key)?;
        Ok(())
    }

    fn get_current_key(&self) -> ArconResult<&IK> {
        Ok(&self.common.curr_key)
    }

    fn set_current_key(&mut self, new_key: IK) -> ArconResult<()> {
        self.common.curr_key = new_key;
        Ok(())
    }

    fn get_current_namespace(&self) -> ArconResult<&N> {
        Ok(&self.common.curr_namespace)
    }

    fn set_current_namespace(&mut self, new_namespace: N) -> ArconResult<()> {
        self.common.curr_namespace = new_namespace;
        Ok(())
    }
}

impl<IK, N, T> ValueState<IK, N, T> for InMemoryValueState<IK, N, T>
    where IK: Serialize, N: Serialize, T: Serialize, T: for<'a> Deserialize<'a> {
    fn get(&self) -> ArconResult<T> {
        let key = self.common.get_db_key(&())?;
        let serialized = self.common.backend_ref.get(&key)?;
        let value = bincode::deserialize(&serialized)
            .map_err(|e| arcon_err_kind!("Cannot deserialize value state: {}", e))?;
        Ok(value)
    }

    fn set(&mut self, new_value: T) -> ArconResult<()> {
        let key = self.common.get_db_key(&())?;
        let serialized = bincode::serialize(&new_value)
            .map_err(|e| arcon_err_kind!("Cannot serialize value state: {}", e))?;
        self.common.backend_ref.put(&key, &serialized)?;
        Ok(())
    }
}

//pub struct InMemoryMapState<IK, N, K, V> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_mem_test() {
        let mut db = InMemory::create_shared("test").unwrap();
        let key = "key";
        let value = "hej";
        let _ = db.put(key.as_bytes(), value.as_bytes()).unwrap();
        let fetched = db.get(key.as_bytes()).unwrap();
        assert_eq!(value, String::from_utf8_lossy(&fetched));
        db.remove(key.as_bytes()).unwrap();
        let res = db.get(key.as_bytes());
        assert!(res.is_err());
    }

    #[test]
    fn in_memory_value_state_test() {
        let db = InMemory::create_shared("test").unwrap();
        let mut value_state = db.new_value_state((), ());

        let unset = value_state.get();
        assert!(unset.is_err());

        value_state.set(123).unwrap();
        let set = value_state.get().unwrap();
        assert_eq!(set, 123);

        value_state.clear().unwrap();
        let cleared = value_state.get();
        assert!(cleared.is_err());
    }

    #[test]
    fn in_memory_value_states_are_independant() {
        let db = InMemory::create_shared("test").unwrap();
        let mut v1 = db.new_value_state((), ());
        let mut v2 = db.new_value_state((), ());

        v1.set(123).unwrap();
        v2.set(456).unwrap();

        let v1v = v1.get().unwrap();
        let v2v = v2.get().unwrap();
        assert_eq!(v1v, 123);
        assert_eq!(v2v, 456);

        v1.clear().unwrap();
        let v1res = v1.get();
        let v2v = v2.get().unwrap();
        assert!(v1res.is_err());
        assert_eq!(v2v, 456);
    }

    #[test]
    fn in_memory_value_states_handle_state_for_different_keys_and_namespaces() {
        let db = InMemory::create_shared("test").unwrap();
        let mut value_state = db.new_value_state(0, 0);

        value_state.set(0).unwrap();
        value_state.set_current_key(1).unwrap();
        let should_be_err = value_state.get();
        assert!(should_be_err.is_err());

        value_state.set(1);
        let should_be_one = value_state.get().unwrap();
        assert_eq!(should_be_one, 1);

        value_state.set_current_key(0);
        let should_be_zero = value_state.get().unwrap();
        assert_eq!(should_be_zero, 0);

        value_state.set_current_namespace(1);
        let should_be_err = value_state.get();
        assert!(should_be_err.is_err());

        value_state.set(2);
        let should_be_two = value_state.get().unwrap();
        assert_eq!(should_be_two, 2);

        value_state.set_current_namespace(0);
        let should_be_zero = value_state.get().unwrap();
        assert_eq!(should_be_zero, 0);
    }
}
