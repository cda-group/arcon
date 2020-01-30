// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::ArconResult,
    state_backend::{
        rocksdb::{state_common::StateCommon, RocksDb},
        state_types,
        state_types::{AggregatingState, Aggregator, AppendingState, MergingState, State},
    },
};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

pub struct RocksDbAggregatingState<IK, N, T, AGG> {
    pub(crate) common: StateCommon<IK, N>,
    pub(crate) aggregator: AGG,
    pub(crate) _phantom: PhantomData<T>,
}

pub(crate) const ACCUMULATOR_MARKER: u8 = 0xAC;
pub(crate) const VALUE_MARKER: u8 = 0x00;

impl<IK, N, T, AGG> State<RocksDb, IK, N> for RocksDbAggregatingState<IK, N, T, AGG>
where
    IK: Serialize,
    N: Serialize,
{
    fn clear(&self, backend: &mut RocksDb) -> ArconResult<()> {
        let key = self.common.get_db_key(&())?;
        backend.remove(&self.common.cf_name, &key)?;
        Ok(())
    }

    delegate_key_and_namespace!(common);
}

impl<IK, N, T, AGG> AppendingState<RocksDb, IK, N, T, AGG::Result>
    for RocksDbAggregatingState<IK, N, T, AGG>
where
    IK: Serialize,
    N: Serialize,
    T: Serialize,
    AGG: Aggregator<T>,
    AGG::Accumulator: Serialize + for<'a> Deserialize<'a>,
{
    fn get(&self, backend: &RocksDb) -> ArconResult<AGG::Result> {
        // TODO: do we want to return R based on a new accumulator if not found?
        let key = self.common.get_db_key(&())?;

        let serialized = backend.get(&self.common.cf_name, &key)?;
        assert_eq!(serialized[0], ACCUMULATOR_MARKER);
        let serialized = &serialized[1..];

        let current_accumulator = bincode::deserialize(serialized).map_err(|e| {
            arcon_err_kind!("Could not deserialize aggregating state accumulator: {}", e)
        })?;
        Ok(self.aggregator.accumulator_into_result(current_accumulator))
    }

    fn append(&self, backend: &mut RocksDb, value: T) -> ArconResult<()> {
        let key = self.common.get_db_key(&())?;
        let mut serialized = vec![VALUE_MARKER];
        bincode::serialize_into(&mut serialized, &value)
            .map_err(|e| arcon_err_kind!("Could not serialize aggregating state value: {}", e))?;

        // see StateCommon::new_for_aggregating_state
        let cf = backend.get_cf_handle(&self.common.cf_name)?;
        backend
            .db
            .merge_cf(cf, key, serialized)
            .map_err(|e| arcon_err_kind!("Could not perform merge operation: {}", e))
    }
}

impl<IK, N, T, AGG> MergingState<RocksDb, IK, N, T, AGG::Result>
    for RocksDbAggregatingState<IK, N, T, AGG>
where
    IK: Serialize,
    N: Serialize,
    T: Serialize,
    AGG: Aggregator<T>,
    AGG::Accumulator: Serialize + for<'a> Deserialize<'a>,
{
}

impl<IK, N, T, AGG> AggregatingState<RocksDb, IK, N, T, AGG::Result>
    for RocksDbAggregatingState<IK, N, T, AGG>
where
    IK: Serialize,
    N: Serialize,
    T: Serialize,
    AGG: Aggregator<T>,
    AGG::Accumulator: Serialize + for<'a> Deserialize<'a>,
{
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::state_backend::{
        rocksdb::tests::TestDb, state_types::ClosuresAggregator, AggregatingStateBuilder,
        StateBackend,
    };
    use tempfile::TempDir;

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
        );

        aggregating_state.append(&mut db, 1).unwrap();
        aggregating_state.append(&mut db, 2).unwrap();
        aggregating_state.append(&mut db, 3).unwrap();

        assert_eq!(aggregating_state.get(&db).unwrap(), "[1, 2, 3]".to_string());
    }
}
