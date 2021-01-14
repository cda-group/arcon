// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
use crate::{
    data::Metakey, error::*, serialization::protobuf, sled::Sled, Aggregator, AggregatorOps,
    AggregatorState, Handle,
};
use sled::MergeOperator;
use std::iter;

pub(crate) const ACCUMULATOR_MARKER: u8 = 0xAC;
pub(crate) const VALUE_MARKER: u8 = 0x00;

impl AggregatorOps for Sled {
    fn aggregator_clear<A: Aggregator, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<AggregatorState<A>, IK, N>,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;
        self.remove(&handle.id, &key)?;
        Ok(())
    }

    fn aggregator_get<A: Aggregator, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<AggregatorState<A>, IK, N>,
    ) -> Result<<A as Aggregator>::Result> {
        let key = handle.serialize_metakeys()?;

        if let Some(serialized) = self.get(&handle.id, &key)? {
            assert_eq!(serialized[0], ACCUMULATOR_MARKER);
            let serialized = &serialized[1..];

            let current_accumulator = protobuf::deserialize(serialized)?;
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
        &self,
        handle: &Handle<AggregatorState<A>, IK, N>,
        value: <A as Aggregator>::Input,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;
        let mut serialized = vec![VALUE_MARKER];
        protobuf::serialize_into(&mut serialized, &value)?;

        // See the make_aggregator_merge function in this module. Its result is set as the merging operator for this state.
        self.tree(&handle.id)?.merge(key, serialized)?;

        Ok(())
    }
}

pub fn make_aggregator_merge<A>(aggregator: A) -> impl MergeOperator + 'static
where
    A: Aggregator,
{
    move |_key: &[u8], existent: Option<&[u8]>, new: &[u8]| {
        let mut all_slices = existent.into_iter().chain(iter::once(new));

        let first = all_slices.next();
        let mut accumulator = {
            match first {
                Some([ACCUMULATOR_MARKER, accumulator_bytes @ ..]) => {
                    protobuf::deserialize(accumulator_bytes).ok()?
                }
                Some([VALUE_MARKER, value_bytes @ ..]) => {
                    let value: A::Input = protobuf::deserialize(value_bytes).ok()?;
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
                    let second_acc = protobuf::deserialize(accumulator_bytes).ok()?;
                    accumulator = aggregator.merge_accumulators(accumulator, second_acc);
                }
                [VALUE_MARKER, value_bytes @ ..] => {
                    let value = protobuf::deserialize(value_bytes).ok()?;
                    aggregator.add(&mut accumulator, value);
                }
                _ => {
                    eprintln!("unknown operand in aggregate merge operator");
                    return None;
                }
            }
        }

        let mut result = vec![ACCUMULATOR_MARKER];
        protobuf::serialize_into(&mut result, &accumulator).ok()?;

        Some(result)
    }
}
