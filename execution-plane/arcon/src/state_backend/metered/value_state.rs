// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::ArconResult,
    state_backend::{
        metered::Metered,
        state_types::{State, ValueState},
    },
};

pub struct MeteredValueState<VS> {
    pub(crate) inner: VS,
}

impl<VS, SB, IK, N> State<Metered<SB>, IK, N> for MeteredValueState<VS>
where
    VS: State<SB, IK, N>,
{
    fn clear(&self, backend: &mut Metered<SB>) -> ArconResult<()> {
        backend.measure_mut("State::clear", |backend| self.inner.clear(backend))
    }

    fn get_current_key(&self) -> ArconResult<&IK> {
        self.inner.get_current_key()
    }

    fn set_current_key(&mut self, new_key: IK) -> ArconResult<()> {
        self.inner.set_current_key(new_key)
    }

    fn get_current_namespace(&self) -> ArconResult<&N> {
        self.inner.get_current_namespace()
    }

    fn set_current_namespace(&mut self, new_namespace: N) -> ArconResult<()> {
        self.inner.set_current_namespace(new_namespace)
    }
}

impl<VS, SB, IK, N, T> ValueState<Metered<SB>, IK, N, T> for MeteredValueState<VS>
where
    VS: ValueState<SB, IK, N, T>,
{
    fn get(&self, backend: &Metered<SB>) -> ArconResult<Option<T>> {
        backend.measure("ValueState::get", |backend| self.inner.get(backend))
    }

    fn set(&self, backend: &mut Metered<SB>, new_value: T) -> ArconResult<()> {
        backend.measure_mut("ValueState::set", move |backend| {
            self.inner.set(backend, new_value)
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::{
        in_memory::InMemory,
        serialization::{NativeEndianBytesDump, Prost},
        StateBackend, ValueStateBuilder,
    };

    #[test]
    fn in_memory_value_state_test() {
        let mut db = Metered::<InMemory>::new("test".as_ref()).unwrap();
        let value_state = db.new_value_state("test_state", (), (), NativeEndianBytesDump, Prost);

        let unset = value_state.get(&db).unwrap();
        assert!(unset.is_none());

        value_state.set(&mut db, 123).unwrap();
        let set = value_state.get(&db).unwrap().unwrap();
        assert_eq!(set, 123);

        value_state.clear(&mut db).unwrap();
        let cleared = value_state.get(&db).unwrap();
        assert!(cleared.is_none());

        println!(
            "test metrics for =={}==\n{:#?}",
            db.backend_name,
            &**db.metrics.borrow()
        )
    }
}
