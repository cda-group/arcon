// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::ArconResult,
    state_backend::{
        in_memory::{InMemory, StateCommon},
        serialization::{DeserializableWith, SerializableFixedSizeWith, SerializableWith},
        state_types::{AggregatingState, Aggregator, AppendingState, MergingState, State},
    },
};
use std::marker::PhantomData;

pub struct InMemoryAggregatingState<IK, N, T, AGG, KS, TS> {
    pub(crate) common: StateCommon<IK, N, KS, TS>,
    pub(crate) aggregator: AGG,
    pub(crate) _phantom: PhantomData<T>,
}

impl<IK, N, T, AGG, KS, TS> State<InMemory, IK, N>
    for InMemoryAggregatingState<IK, N, T, AGG, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
{
    fn clear(&self, backend: &mut InMemory) -> ArconResult<()> {
        let key = self.common.get_db_key_prefix()?;
        backend.remove(&key)?;
        Ok(())
    }

    delegate_key_and_namespace!(common);
}

impl<IK, N, T, AGG, KS, TS> AppendingState<InMemory, IK, N, T, AGG::Result>
    for InMemoryAggregatingState<IK, N, T, AGG, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    AGG: Aggregator<T>,
    AGG::Accumulator: SerializableWith<TS> + DeserializableWith<TS>,
{
    fn get(&self, backend: &InMemory) -> ArconResult<AGG::Result> {
        // TODO: do we want to return R based on a new accumulator if not found?
        let key = self.common.get_db_key_prefix()?;
        let serialized = backend.get(&key)?;
        let current_accumulator =
            AGG::Accumulator::deserialize(&self.common.value_serializer, serialized)?;
        Ok(self.aggregator.accumulator_into_result(current_accumulator))
    }

    fn append(&self, backend: &mut InMemory, value: T) -> ArconResult<()> {
        let key = self.common.get_db_key_prefix()?;
        let accumulator_buffer = backend.get_mut_or_init_empty(&key)?;

        let mut current_accumulator = if accumulator_buffer.is_empty() {
            self.aggregator.create_accumulator()
        } else {
            AGG::Accumulator::deserialize(&self.common.value_serializer, &*accumulator_buffer)?
        };

        self.aggregator.add(&mut current_accumulator, value);
        accumulator_buffer.clear();

        AGG::Accumulator::serialize_into(
            &self.common.value_serializer,
            accumulator_buffer,
            &current_accumulator,
        )?;

        Ok(())
    }
}

impl<IK, N, T, AGG, KS, TS> MergingState<InMemory, IK, N, T, AGG::Result>
    for InMemoryAggregatingState<IK, N, T, AGG, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    AGG: Aggregator<T>,
    AGG::Accumulator: SerializableWith<TS> + DeserializableWith<TS>,
{
}

impl<IK, N, T, AGG, KS, TS> AggregatingState<InMemory, IK, N, T, AGG::Result>
    for InMemoryAggregatingState<IK, N, T, AGG, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    AGG: Aggregator<T>,
    AGG::Accumulator: SerializableWith<TS> + DeserializableWith<TS>,
{
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::{
        serialization::Bincode, state_types::ClosuresAggregator, AggregatingStateBuilder,
        StateBackend,
    };

    #[test]
    fn aggregating_state_test() {
        let mut db = InMemory::new("test").unwrap();
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
            Bincode,
            Bincode,
        );

        aggregating_state.append(&mut db, 1).unwrap();
        aggregating_state.append(&mut db, 2).unwrap();
        aggregating_state.append(&mut db, 3).unwrap();

        assert_eq!(aggregating_state.get(&db).unwrap(), "[1, 2, 3]".to_string());
    }
}
