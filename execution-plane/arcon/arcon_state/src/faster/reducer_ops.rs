// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
use crate::{
    error::*, faster::AggregatorFn, serialization::protobuf, Faster, Handle, Metakey, Reducer,
    ReducerOps, ReducerState, Value,
};

impl ReducerOps for Faster {
    fn reducer_clear<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<ReducerState<T, F>, IK, N>,
    ) -> Result<()> {
        let key = handle.serialize_id_and_metakeys()?;
        self.remove(&key)?;
        Ok(())
    }

    fn reducer_get<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ReducerState<T, F>, IK, N>,
    ) -> Result<Option<T>> {
        let key = handle.serialize_id_and_metakeys()?;
        if let Some(storage) = self.get_agg(&key)? {
            let value = protobuf::deserialize(&*storage)?;
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn reducer_reduce<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<ReducerState<T, F>, IK, N>,
        value: T,
    ) -> Result<()> {
        let key = handle.serialize_id_and_metakeys()?;
        let serialized = protobuf::serialize(&value)?;

        self.aggregate(&key, serialized, handle.id)
    }
}

pub fn make_reduce_fn<T, F>(reducer: F) -> Box<AggregatorFn>
where
    T: Value,
    F: Reducer<T>,
{
    Box::new(move |old, new| {
        let old = protobuf::deserialize(old)?;
        let new = protobuf::deserialize(new)?;
        let res = reducer(&old, &new);

        protobuf::serialize(&res)
    })
}
