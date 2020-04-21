// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::ArconResult,
    state_backend::{
        metered::Metered,
        state_types::{AggregatingState, AppendingState, MergingState, State},
    },
};

pub struct MeteredAggregatingState<AS> {
    pub(crate) inner: AS,
}
impl_metered_state!(MeteredAggregatingState<AS>: AggregatingState);

impl<SB, AS, IK, N, T, R> AppendingState<Metered<SB>, IK, N, T, R> for MeteredAggregatingState<AS>
where
    AS: AppendingState<SB, IK, N, T, R>,
{
    measure_delegated! { AggregatingState:
        fn get(&self, backend: &Metered<SB>) -> ArconResult<R>;
        fn append(&self, backend: &mut Metered<SB>, value: T) -> ArconResult<()>;
    }
}

impl<SB, AS, IK, N, T, R> MergingState<Metered<SB>, IK, N, T, R> for MeteredAggregatingState<AS> where
    AS: MergingState<SB, IK, N, T, R>
{
}

impl<SB, AS, IK, N, T, R> AggregatingState<Metered<SB>, IK, N, T, R> for MeteredAggregatingState<AS> where
    AS: AggregatingState<SB, IK, N, T, R>
{
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::{
        in_memory::InMemory,
        serialization::{NativeEndianBytesDump, Prost},
        state_types::ClosuresAggregator,
        AggregatingStateBuilder, StateBackend,
    };

    #[test]
    fn aggregating_state_test() {
        let mut db = Metered::<InMemory>::new("test".as_ref()).unwrap();
        let aggregating_state = db.new_aggregating_state(
            "test_state",
            (),
            (),
            ClosuresAggregator::new(
                || vec![],
                Vec::push,
                |mut fst, mut snd| {
                    fst.append(&mut snd);
                    fst
                },
                |v| format!("{:?}", v),
            ),
            NativeEndianBytesDump,
            Prost,
        );

        aggregating_state.append(&mut db, 1).unwrap();
        aggregating_state.append(&mut db, 2).unwrap();
        aggregating_state.append(&mut db, 3).unwrap();

        assert_eq!(aggregating_state.get(&db).unwrap(), "[1, 2, 3]".to_string());

        println!(
            "test metrics for =={}==\n{:#?}",
            db.backend_name,
            &**db.metrics.borrow()
        )
    }
}
