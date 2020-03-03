// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::ArconResult,
    state_backend::{
        in_memory::{InMemory, StateCommon},
        serialization::SerializableFixedSizeWith,
        state_types::{State, ValueState},
    },
};
use smallbox::SmallBox;
use std::marker::PhantomData;

pub struct InMemoryValueState<IK, N, T, KS> {
    pub(crate) common: StateCommon<IK, N, KS>,
    pub(crate) _phantom: PhantomData<T>,
}

impl<IK, N, T, KS> State<InMemory, IK, N> for InMemoryValueState<IK, N, T, KS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
{
    fn clear(&self, backend: &mut InMemory) -> ArconResult<()> {
        let key = self.common.get_db_key_prefix()?;
        let _value = backend.remove(&key);
        Ok(())
    }

    delegate_key_and_namespace!(common);
}

impl<IK, N, T, KS> ValueState<InMemory, IK, N, T> for InMemoryValueState<IK, N, T, KS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    T: Send + Sync + Clone + 'static,
{
    fn get(&self, backend: &InMemory) -> ArconResult<Option<T>> {
        let key = self.common.get_db_key_prefix()?;
        if let Some(dynamic) = backend.get(&key) {
            let value = dynamic
                .downcast_ref::<T>()
                .ok_or_else(|| arcon_err_kind!("Dynamic value has a wrong type!"))?
                .clone();
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn set(&self, backend: &mut InMemory, new_value: T) -> ArconResult<()> {
        let key = self.common.get_db_key_prefix()?;
        let dynamic = SmallBox::new(new_value);
        let _old_value = backend.insert(key, dynamic);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::{
        serialization::{NativeEndianBytesDump, Prost},
        StateBackend, ValueStateBuilder,
    };

    #[test]
    fn in_memory_value_state_test() {
        let mut db = InMemory::new("test").unwrap();
        let value_state = db.new_value_state("test_state", (), (), NativeEndianBytesDump, Prost);

        let unset = value_state.get(&db).unwrap();
        assert!(unset.is_none());

        value_state.set(&mut db, 123).unwrap();
        let set = value_state.get(&db).unwrap().unwrap();
        assert_eq!(set, 123);

        value_state.clear(&mut db).unwrap();
        let cleared = value_state.get(&db).unwrap();
        assert!(cleared.is_none());
    }

    #[test]
    fn in_memory_value_states_are_independant() {
        let mut db = InMemory::new("test").unwrap();
        let v1 = db.new_value_state("test1", (), (), NativeEndianBytesDump, Prost);
        let v2 = db.new_value_state("test2", (), (), NativeEndianBytesDump, Prost);

        v1.set(&mut db, 123).unwrap();
        v2.set(&mut db, 456).unwrap();

        let v1v = v1.get(&db).unwrap().unwrap();
        let v2v = v2.get(&db).unwrap().unwrap();
        assert_eq!(v1v, 123);
        assert_eq!(v2v, 456);

        v1.clear(&mut db).unwrap();
        let v1opt = v1.get(&db).unwrap();
        let v2v = v2.get(&db).unwrap().unwrap();
        assert!(v1opt.is_none());
        assert_eq!(v2v, 456);
    }

    #[test]
    fn in_memory_value_states_handle_state_for_different_keys_and_namespaces() {
        let mut db = InMemory::new("test").unwrap();
        let mut value_state = db.new_value_state("test_state", 0, 0, NativeEndianBytesDump, Prost);

        value_state.set(&mut db, 0).unwrap();
        value_state.set_current_key(1).unwrap();
        let should_be_none = value_state.get(&db).unwrap();
        assert!(should_be_none.is_none());

        value_state.set(&mut db, 1).unwrap();
        let should_be_one = value_state.get(&db).unwrap().unwrap();
        assert_eq!(should_be_one, 1);

        value_state.set_current_key(0).unwrap();
        let should_be_zero = value_state.get(&db).unwrap().unwrap();
        assert_eq!(should_be_zero, 0);

        value_state.set_current_namespace(1).unwrap();
        let should_be_none = value_state.get(&db).unwrap();
        assert!(should_be_none.is_none());

        value_state.set(&mut db, 2).unwrap();
        let should_be_two = value_state.get(&db).unwrap().unwrap();
        assert_eq!(should_be_two, 2);

        value_state.set_current_namespace(0).unwrap();
        let should_be_zero = value_state.get(&db).unwrap().unwrap();
        assert_eq!(should_be_zero, 0);
    }
}
