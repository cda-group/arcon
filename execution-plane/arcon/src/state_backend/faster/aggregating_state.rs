// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::ArconResult,
    state_backend::{
        faster::{Faster, StateCommon},
        serialization::{DeserializableWith, SerializableWith},
        state_types::{AggregatingState, Aggregator, AppendingState, MergingState, State},
    },
};

use std::marker::PhantomData;

pub struct FasterAggregatingState<IK, N, T, AGG, KS, TS> {
    pub(crate) common: StateCommon<IK, N, KS, TS>,
    pub(crate) aggregator: AGG,
    pub(crate) aggregate_fn: Box<dyn Fn(&[u8], &[u8]) -> Vec<u8> + Send + Sync>,
    pub(crate) _phantom: PhantomData<T>,
}

pub(crate) const ACCUMULATOR_MARKER: u8 = 0xAC;
pub(crate) const VALUE_MARKER: u8 = 0x00;

impl<IK, N, T, AGG, KS, TS> State<Faster, IK, N> for FasterAggregatingState<IK, N, T, AGG, KS, TS>
where
    IK: SerializableWith<KS>,
    N: SerializableWith<KS>,
{
    fn clear(&self, backend: &mut Faster) -> ArconResult<()> {
        backend.in_session_mut(|backend| {
            let key = self.common.get_db_key_prefix()?;
            backend.remove(&key)?;
            Ok(())
        })
    }

    delegate_key_and_namespace!(common);
}

pub fn make_aggregate_fn<T, AGG, TS>(
    aggregator: AGG,
    value_serializer: TS,
) -> Box<dyn Fn(&[u8], &[u8]) -> Vec<u8> + Send + Sync>
where
    T: DeserializableWith<TS>,
    AGG: Aggregator<T> + Send + Sync + 'static,
    AGG::Accumulator: SerializableWith<TS> + DeserializableWith<TS>,
    TS: Send + Sync + 'static,
{
    Box::new(move |old, new| {
        let mut accumulator = {
            match old {
                [ACCUMULATOR_MARKER, accumulator_bytes @ ..] => {
                    AGG::Accumulator::deserialize(&value_serializer, accumulator_bytes)
                        .expect("accumulator deserialization error")
                }
                [VALUE_MARKER, value_bytes @ ..] => {
                    let value: T = T::deserialize(&value_serializer, value_bytes)
                        .expect("value deserialization error");
                    let mut acc = aggregator.create_accumulator();
                    aggregator.add(&mut acc, value);
                    acc
                }
                _ => {
                    panic!("unknown operand in aggregate merge operator");
                }
            }
        };

        match new {
            [ACCUMULATOR_MARKER, accumulator_bytes @ ..] => {
                let second_acc =
                    AGG::Accumulator::deserialize(&value_serializer, accumulator_bytes)
                        .expect("accumulator deserialization error");

                accumulator = aggregator.merge_accumulators(accumulator, second_acc);
            }
            [VALUE_MARKER, value_bytes @ ..] => {
                let value = T::deserialize(&value_serializer, value_bytes)
                    .expect("value deserialization error");

                aggregator.add(&mut accumulator, value);
            }
            _ => {
                panic!("unknown operand in aggregate merge operator");
            }
        }

        let mut result = Vec::with_capacity(
            1 + AGG::Accumulator::size_hint(&value_serializer, &accumulator).unwrap_or(0),
        );
        result.push(ACCUMULATOR_MARKER);

        AGG::Accumulator::serialize_into(&value_serializer, &mut result, &accumulator)
            .expect("accumulator serialization error");

        result
    })
}

impl<IK, N, T, AGG, KS, TS> AppendingState<Faster, IK, N, T, AGG::Result>
    for FasterAggregatingState<IK, N, T, AGG, KS, TS>
where
    IK: SerializableWith<KS>,
    N: SerializableWith<KS>,
    T: SerializableWith<TS>,
    AGG: Aggregator<T>,
    AGG::Accumulator: SerializableWith<TS> + DeserializableWith<TS>,
{
    fn get(&self, backend: &Faster) -> ArconResult<AGG::Result> {
        backend.in_session(|backend| {
            let key = self.common.get_db_key_prefix()?;

            if let Some(serialized) = backend.get_agg(&key)? {
                assert_eq!(serialized[0], ACCUMULATOR_MARKER);
                let serialized = &serialized[1..];

                let current_accumulator =
                    AGG::Accumulator::deserialize(&self.common.value_serializer, serialized)?;
                Ok(self.aggregator.accumulator_into_result(current_accumulator))
            } else {
                Ok(self
                    .aggregator
                    .accumulator_into_result(self.aggregator.create_accumulator()))
            }
        })
    }

    fn append(&self, backend: &mut Faster, value: T) -> ArconResult<()> {
        backend.in_session_mut(|backend| {
            let key = self.common.get_db_key_prefix()?;
            let mut serialized = Vec::with_capacity(
                1 + T::size_hint(&self.common.value_serializer, &value).unwrap_or(0),
            );
            serialized.push(VALUE_MARKER);
            T::serialize_into(&self.common.value_serializer, &mut serialized, &value)?;

            // See the make_aggregating_merge function in this module. Its result is set as the merging operator for this state.
            backend.aggregate(&key, serialized, &*self.aggregate_fn)
        })
    }
}

impl<IK, N, T, AGG, KS, TS> MergingState<Faster, IK, N, T, AGG::Result>
    for FasterAggregatingState<IK, N, T, AGG, KS, TS>
where
    IK: SerializableWith<KS>,
    N: SerializableWith<KS>,
    T: SerializableWith<TS>,
    AGG: Aggregator<T>,
    AGG::Accumulator: SerializableWith<TS> + DeserializableWith<TS>,
{
}

impl<IK, N, T, AGG, KS, TS> AggregatingState<Faster, IK, N, T, AGG::Result>
    for FasterAggregatingState<IK, N, T, AGG, KS, TS>
where
    IK: SerializableWith<KS>,
    N: SerializableWith<KS>,
    T: SerializableWith<TS>,
    AGG: Aggregator<T>,
    AGG::Accumulator: SerializableWith<TS> + DeserializableWith<TS>,
{
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::{
        faster::test::TestDb, serialization::NativeEndianBytesDump,
        state_types::ClosuresAggregator, AggregatingStateBuilder,
    };

    #[test]
    fn aggregating_state_test() {
        let mut db = TestDb::new();
        let aggregating_state = db.new_aggregating_state(
            "test_state",
            (),
            (),
            ClosuresAggregator::new(
                Vec::<u32>::new,
                Vec::push,
                |mut fst, mut snd| {
                    fst.append(&mut snd);
                    fst
                },
                |v| format!("{:?}", v),
            ),
            NativeEndianBytesDump,
            NativeEndianBytesDump,
        );

        aggregating_state.append(&mut db, 1).unwrap();
        aggregating_state.append(&mut db, 2).unwrap();
        aggregating_state.append(&mut db, 3).unwrap();

        assert_eq!(aggregating_state.get(&db).unwrap(), "[1, 2, 3]".to_string());
    }
}
