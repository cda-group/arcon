// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::ArconResult,
    state_backend::{
        in_memory::{InMemory, StateCommon},
        serialization::SerializableFixedSizeWith,
        state_types::{AggregatingState, Aggregator, AppendingState, MergingState, State},
    },
};
use smallbox::SmallBox;
use std::marker::PhantomData;

pub struct InMemoryAggregatingState<IK, N, T, AGG, KS> {
    pub(crate) common: StateCommon<IK, N, KS>,
    pub(crate) aggregator: AGG,
    pub(crate) _phantom: PhantomData<T>,
}

impl<IK, N, T, AGG, KS> State<InMemory, IK, N> for InMemoryAggregatingState<IK, N, T, AGG, KS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
{
    fn clear(&self, backend: &mut InMemory) -> ArconResult<()> {
        let key = self.common.get_db_key_prefix()?;
        let _old_value = backend.remove(&key);
        Ok(())
    }

    delegate_key_and_namespace!(common);
}

impl<IK, N, T, AGG, KS> AppendingState<InMemory, IK, N, T, AGG::Result>
    for InMemoryAggregatingState<IK, N, T, AGG, KS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    AGG: Aggregator<T>,
    AGG::Accumulator: Send + Sync + Clone + 'static,
{
    fn get(&self, backend: &InMemory) -> ArconResult<AGG::Result> {
        let key = self.common.get_db_key_prefix()?;
        if let Some(dynamic) = backend.get(&key) {
            let current_accumulator = dynamic
                .downcast_ref::<AGG::Accumulator>()
                .ok_or_else(|| arcon_err_kind!("Dynamic value has a wrong type!"))?
                .clone();
            Ok(self.aggregator.accumulator_into_result(current_accumulator))
        } else {
            Ok(self
                .aggregator
                .accumulator_into_result(self.aggregator.create_accumulator()))
        }
    }

    fn append(&self, backend: &mut InMemory, value: T) -> ArconResult<()> {
        let key = self.common.get_db_key_prefix()?;
        let current_accumulator = backend
            .get_mut_or_insert(key, || SmallBox::new(self.aggregator.create_accumulator()))
            .downcast_mut::<AGG::Accumulator>()
            .ok_or_else(|| arcon_err_kind!("Dynamic value has a wrong type!"))?;

        self.aggregator.add(current_accumulator, value);

        Ok(())
    }
}

impl<IK, N, T, AGG, KS> MergingState<InMemory, IK, N, T, AGG::Result>
    for InMemoryAggregatingState<IK, N, T, AGG, KS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    AGG: Aggregator<T>,
    AGG::Accumulator: Send + Sync + Clone + 'static,
{
}

impl<IK, N, T, AGG, KS> AggregatingState<InMemory, IK, N, T, AGG::Result>
    for InMemoryAggregatingState<IK, N, T, AGG, KS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    AGG: Aggregator<T>,
    AGG::Accumulator: Send + Sync + Clone + 'static,
{
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::{
        builders::AggregatingStateBuilder,
        serialization::{NativeEndianBytesDump, Prost},
        state_types::ClosuresAggregator,
        StateBackend,
    };

    #[test]
    fn aggregating_state_test() {
        let mut db = InMemory::new("test".as_ref()).unwrap();
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
    }
}
