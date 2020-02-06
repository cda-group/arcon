// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use super::rocksdb::{merge_operator::MergeFn, MergeOperands};
use crate::{
    prelude::ArconResult,
    state_backend::{
        rocksdb::{state_common::StateCommon, RocksDb},
        serialization::{DeserializableWith, SerializableFixedSizeWith, SerializableWith},
        state_types::{AggregatingState, Aggregator, AppendingState, MergingState, State},
    },
};

use std::marker::PhantomData;

pub struct RocksDbAggregatingState<IK, N, T, AGG, KS, TS> {
    pub(crate) common: StateCommon<IK, N, KS, TS>,
    pub(crate) aggregator: AGG,
    pub(crate) _phantom: PhantomData<T>,
}

pub(crate) const ACCUMULATOR_MARKER: u8 = 0xAC;
pub(crate) const VALUE_MARKER: u8 = 0x00;

impl<IK, N, T, AGG, KS, TS> State<RocksDb, IK, N, KS, TS>
    for RocksDbAggregatingState<IK, N, T, AGG, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    (): SerializableWith<KS>,
{
    fn clear(&self, backend: &mut RocksDb) -> ArconResult<()> {
        let key = self.common.get_db_key(&())?;
        backend.remove(&self.common.cf_name, &key)?;
        Ok(())
    }

    delegate_key_and_namespace!(common);
}

pub fn make_aggregating_merge<T, AGG, TS>(
    aggregator: AGG,
    value_serializer: TS,
) -> impl MergeFn + Clone
where
    T: DeserializableWith<TS>,
    AGG: Aggregator<T> + Send + Sync + Clone + 'static,
    AGG::Accumulator: SerializableWith<TS> + DeserializableWith<TS>,
    TS: Send + Sync + Clone + 'static,
{
    move |_key: &[u8], first: Option<&[u8]>, rest: &mut MergeOperands| {
        let mut all_slices = first.into_iter().chain(rest).fuse();

        let first = all_slices.next();
        let mut accumulator = {
            match first {
                Some([ACCUMULATOR_MARKER, accumulator_bytes @ ..]) => {
                    AGG::Accumulator::deserialize(&value_serializer, accumulator_bytes).ok()?
                }
                Some([VALUE_MARKER, value_bytes @ ..]) => {
                    let value: T = T::deserialize(&value_serializer, value_bytes).ok()?;
                    let mut acc = aggregator.create_accumulator();
                    aggregator.add(&mut acc, value);
                    acc
                }
                Some(_) => {
                    eprintln!("unknown operand in aggregate merge operator");
                    return None;
                }
                None => aggregator.create_accumulator(),
            }
        };

        for slice in all_slices {
            match slice {
                [ACCUMULATOR_MARKER, accumulator_bytes @ ..] => {
                    let second_acc =
                        AGG::Accumulator::deserialize(&value_serializer, accumulator_bytes).ok()?;

                    accumulator = aggregator.merge_accumulators(accumulator, second_acc);
                }
                [VALUE_MARKER, value_bytes @ ..] => {
                    let value = T::deserialize(&value_serializer, value_bytes).ok()?;

                    aggregator.add(&mut accumulator, value);
                }
                _ => {
                    eprintln!("unknown operand in aggregate merge operator");
                    return None;
                }
            }
        }

        let mut result = vec![ACCUMULATOR_MARKER];
        AGG::Accumulator::serialize_into(&value_serializer, &mut result, &accumulator).ok()?;

        Some(result)
    }
}

impl<IK, N, T, AGG, KS, TS> AppendingState<RocksDb, IK, N, T, AGG::Result, KS, TS>
    for RocksDbAggregatingState<IK, N, T, AGG, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    (): SerializableWith<KS>,
    T: SerializableWith<TS>,
    AGG: Aggregator<T>,
    AGG::Accumulator: SerializableWith<TS> + DeserializableWith<TS>,
{
    fn get(&self, backend: &RocksDb) -> ArconResult<AGG::Result> {
        // TODO: do we want to return R based on a new/empty accumulator if not found?
        let key = self.common.get_db_key(&())?;

        let serialized = backend.get(&self.common.cf_name, &key)?;
        assert_eq!(serialized[0], ACCUMULATOR_MARKER);
        let serialized = &serialized[1..];

        let current_accumulator =
            AGG::Accumulator::deserialize(&self.common.value_serializer, serialized)?;
        Ok(self.aggregator.accumulator_into_result(current_accumulator))
    }

    fn append(&self, backend: &mut RocksDb, value: T) -> ArconResult<()> {
        let backend = backend.initialized_mut()?;

        let key = self.common.get_db_key(&())?;
        let mut serialized = vec![VALUE_MARKER];
        T::serialize_into(&self.common.value_serializer, &mut serialized, &value)?;

        let cf = backend.get_cf_handle(&self.common.cf_name)?;
        // See the make_aggregating_merge function in this module. Its result is set as the merging operator for this state.
        backend
            .db
            .merge_cf(cf, key, serialized)
            .map_err(|e| arcon_err_kind!("Could not perform merge operation: {}", e))
    }
}

impl<IK, N, T, AGG, KS, TS> MergingState<RocksDb, IK, N, T, AGG::Result, KS, TS>
    for RocksDbAggregatingState<IK, N, T, AGG, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    (): SerializableWith<KS>,
    T: SerializableWith<TS>,
    AGG: Aggregator<T>,
    AGG::Accumulator: SerializableWith<TS> + DeserializableWith<TS>,
{
}

impl<IK, N, T, AGG, KS, TS> AggregatingState<RocksDb, IK, N, T, AGG::Result, KS, TS>
    for RocksDbAggregatingState<IK, N, T, AGG, KS, TS>
where
    IK: SerializableFixedSizeWith<KS>,
    N: SerializableFixedSizeWith<KS>,
    (): SerializableWith<KS>,
    T: SerializableWith<TS>,
    AGG: Aggregator<T>,
    AGG::Accumulator: SerializableWith<TS> + DeserializableWith<TS>,
{
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::{
        rocksdb::tests::TestDb, serialization::Bincode, state_types::ClosuresAggregator,
        AggregatingStateBuilder,
    };

    #[test]
    fn aggregating_state_test() {
        let mut db = TestDb::new();
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
