// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use std::marker::PhantomData;
use serde::{Serialize, Deserialize};
use crate::{
    state_backend::{
        in_memory::{StateCommon, InMemory},
        state_types::{State, ValueState},
    },
    prelude::ArconResult,
};

pub struct InMemoryValueState<IK, N, T> {
    pub(crate) common: StateCommon<IK, N>,
    pub(crate) _phantom: PhantomData<T>,
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
        backend.put(key, serialized)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::{ValueStateBuilder, StateBackend};

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
}