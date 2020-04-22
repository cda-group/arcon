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

impl_metered_state!(MeteredValueState<VS>: ValueState);

impl<VS, SB, IK, N, T> ValueState<Metered<SB>, IK, N, T> for MeteredValueState<VS>
where
    VS: ValueState<SB, IK, N, T>,
{
    measure_delegated! { ValueState:
        fn get(&self, backend: &Metered<SB>) -> ArconResult<Option<T>>;
        fn set(&self, backend: &mut Metered<SB>, new_value: T) -> ArconResult<()>;
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
