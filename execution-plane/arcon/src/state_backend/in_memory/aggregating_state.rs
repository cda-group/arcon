use std::marker::PhantomData;
use serde::{Serialize, Deserialize};
use crate::{
    state_backend::{
        in_memory::{StateCommon, InMemory},
        state_types::{State, AppendingState, Aggregator, MergingState, AggregatingState},
        StateBackend
    },
    prelude::ArconResult,
};

pub struct InMemoryAggregatingState<IK, N, T, AGG> {
    pub(crate) common: StateCommon<IK, N>,
    pub(crate) aggregator: AGG,
    pub(crate) _phantom: PhantomData<T>,
}

impl<IK, N, T, AGG> State<InMemory, IK, N> for InMemoryAggregatingState<IK, N, T, AGG>
    where IK: Serialize, N: Serialize
{
    fn clear(&self, backend: &mut InMemory) -> ArconResult<()> {
        let key = self.common.get_db_key(&())?;
        backend.remove(&key)?;
        Ok(())
    }

    delegate_key_and_namespace!(common);
}

impl<IK, N, T, AGG> AppendingState<InMemory, IK, N, T, AGG::Result> for InMemoryAggregatingState<IK, N, T, AGG>
    where IK: Serialize, N: Serialize, AGG: Aggregator<T>, AGG::Accumulator: Serialize + for<'a> Deserialize<'a> {
    fn get(&self, backend: &InMemory) -> ArconResult<AGG::Result> {
        // TODO: do we want to return R based on a new accumulator if not found?
        let key = self.common.get_db_key(&())?;
        let serialized = backend.get(&key)?;
        let current_accumulator = bincode::deserialize(serialized)
            .map_err(|e| arcon_err_kind!("Could not deserialize aggregating state accumulator: {}", e))?;
        Ok(self.aggregator.accumulator_into_result(current_accumulator))
    }

    fn append(&self, backend: &mut InMemory, value: T) -> ArconResult<()> {
        let key = self.common.get_db_key(&())?;
        let accumulator_buffer = backend.get_mut_or_init_empty(&key)?;

        let mut current_accumulator = if accumulator_buffer.is_empty() {
            self.aggregator.create_accumulator()
        } else {
            bincode::deserialize(&*accumulator_buffer)
                .map_err(|e| arcon_err_kind!("Could not deserialize aggregating state accumulator: {}", e))?
        };

        self.aggregator.add(&mut current_accumulator, value);
        accumulator_buffer.clear();

        bincode::serialize_into(accumulator_buffer, &current_accumulator)
            .map_err(|e| arcon_err_kind!("Could not serialize aggregating state accumulator: {}", e))?;

        Ok(())
    }
}

impl<IK, N, T, AGG> MergingState<InMemory, IK, N, T, AGG::Result> for InMemoryAggregatingState<IK, N, T, AGG>
    where IK: Serialize, N: Serialize, AGG: Aggregator<T>, AGG::Accumulator: Serialize + for<'a> Deserialize<'a> {}

impl<IK, N, T, AGG> AggregatingState<InMemory, IK, N, T, AGG::Result> for InMemoryAggregatingState<IK, N, T, AGG>
    where IK: Serialize, N: Serialize, AGG: Aggregator<T>, AGG::Accumulator: Serialize + for<'a> Deserialize<'a> {}
