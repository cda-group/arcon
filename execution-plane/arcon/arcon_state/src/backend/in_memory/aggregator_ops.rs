// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
use crate::{
    backend::{Aggregator, AggregatorOps, AggregatorState, Handle, InMemory, Metakey},
    error::*,
};
use smallbox::SmallBox;

impl AggregatorOps for InMemory {
    fn aggregator_clear<A: Aggregator, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<AggregatorState<A>, IK, N>,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;
        let _old_value = self.get_mut(handle).remove(&key);
        Ok(())
    }

    fn aggregator_get<A: Aggregator, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<AggregatorState<A>, IK, N>,
    ) -> Result<<A as Aggregator>::Result> {
        let key = handle.serialize_metakeys()?;
        if let Some(dynamic) = self.get(handle).get(&key) {
            let current_accumulator = dynamic
                .downcast_ref::<A::Accumulator>()
                .context(InMemoryWrongType)?
                .clone();
            Ok(handle
                .extra_data
                .accumulator_into_result(current_accumulator))
        } else {
            Ok(handle
                .extra_data
                .accumulator_into_result(handle.extra_data.create_accumulator()))
        }
    }

    fn aggregator_aggregate<A: Aggregator, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<AggregatorState<A>, IK, N>,
        value: <A as Aggregator>::Input,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;
        let current_accumulator = self
            .get_mut(handle)
            .entry(key)
            .or_insert_with(|| SmallBox::new(handle.extra_data.create_accumulator()))
            .downcast_mut::<A::Accumulator>()
            .context(InMemoryWrongType)?;

        handle.extra_data.add(current_accumulator, value);

        Ok(())
    }
}
