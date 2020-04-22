// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::ArconResult,
    state_backend::{
        metered::Metered,
        state_types::{AppendingState, MergingState, ReducingState, State},
    },
};

pub struct MeteredReducingState<RS> {
    pub(crate) inner: RS,
}

impl_metered_state!(MeteredReducingState<RS>: ReducingState);

impl<SB, RS, IK, N, T> AppendingState<Metered<SB>, IK, N, T, Option<T>> for MeteredReducingState<RS>
where
    RS: AppendingState<SB, IK, N, T, Option<T>>,
{
    measure_delegated! { ReducingState:
        fn get(&self, backend: &Metered<SB>) -> ArconResult<Option<T>>;
        fn append(&self, backend: &mut Metered<SB>, value: T) -> ArconResult<()>;
    }
}

impl<SB, RS, IK, N, T> MergingState<Metered<SB>, IK, N, T, Option<T>> for MeteredReducingState<RS> where
    RS: MergingState<SB, IK, N, T, Option<T>>
{
}

impl<SB, RS, IK, N, T> ReducingState<Metered<SB>, IK, N, T> for MeteredReducingState<RS> where
    RS: ReducingState<SB, IK, N, T>
{
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::{
        in_memory::InMemory,
        serialization::{NativeEndianBytesDump, Prost},
        ReducingStateBuilder, StateBackend,
    };

    #[test]
    fn reducing_state_test() {
        let mut db = Metered::<InMemory>::new("test".as_ref()).unwrap();
        let reducing_state = db.new_reducing_state(
            "test_state",
            (),
            (),
            |old: &i32, new: &i32| *old.max(new),
            NativeEndianBytesDump,
            Prost,
        );

        reducing_state.append(&mut db, 7).unwrap();
        reducing_state.append(&mut db, 42).unwrap();
        reducing_state.append(&mut db, 10).unwrap();

        assert_eq!(reducing_state.get(&db).unwrap().unwrap(), 42);

        println!(
            "test metrics for =={}==\n{:#?}",
            db.backend_name,
            &**db.metrics.borrow()
        )
    }
}
