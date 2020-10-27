// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
use crate::{
    error::*, rocks::default_write_opts, serialization::protobuf, Aggregator, AggregatorOps,
    AggregatorState, Handle, Metakey, Rocks,
};
use rocksdb::{merge_operator::MergeFn, MergeOperands};

pub(crate) const ACCUMULATOR_MARKER: u8 = 0xAC;
pub(crate) const VALUE_MARKER: u8 = 0x00;

impl AggregatorOps for Rocks {
    fn aggregator_clear<A: Aggregator, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<AggregatorState<A>, IK, N>,
    ) -> Result<()> {
        let key = handle.serialize_metakeys()?;
        self.remove(handle.id, &key)?;
        Ok(())
    }

    fn aggregator_get<A: Aggregator, IK: Metakey, N: Metakey>(
        &self, handle: &Handle<AggregatorState<A>, IK, N>,
    ) -> Result<<A as Aggregator>::Result> {
        let key = handle.serialize_metakeys()?;

        if let Some(serialized) = self.get(handle.id, &key)? {
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
        let mut serialized = Vec::with_capacity(protobuf::size_hint(&value).unwrap_or(0) + 1);
        serialized.push(VALUE_MARKER);
        protobuf::serialize_into(&mut serialized, &value)?;

        let cf = self.get_cf_handle(handle.id)?;
        // See the make_aggregating_merge function in this module. Its result is set as the
        // merging operator for this state.
        Ok(self
            .db()
            .merge_cf_opt(cf, key, serialized, &default_write_opts())?)
    }
}

pub(crate) fn make_aggregator_merge<A>(aggregator: A) -> impl MergeFn + Clone
where
    A: Aggregator,
{
    move |_key: &[u8], first: Option<&[u8]>, rest: &mut MergeOperands| {
        let mut all_slices = first.into_iter().chain(rest).fuse();

        let first = all_slices.next();
        let mut accumulator = {
            match first {
                Some([ACCUMULATOR_MARKER, accumulator_bytes @ ..]) => {
                    protobuf::deserialize::<A::Accumulator>(accumulator_bytes).ok()?
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
                    let second_acc: A::Accumulator =
                        protobuf::deserialize(accumulator_bytes).ok()?;

                    accumulator = aggregator.merge_accumulators(accumulator, second_acc);
                }
                [VALUE_MARKER, value_bytes @ ..] => {
                    let value: A::Input = protobuf::deserialize(value_bytes).ok()?;

                    aggregator.add(&mut accumulator, value);
                }
                _ => {
                    eprintln!("unknown operand in aggregate merge operator");
                    return None;
                }
            }
        }

        let mut result = Vec::with_capacity(1 + protobuf::size_hint(&accumulator).unwrap_or(0));
        result.push(ACCUMULATOR_MARKER);

        protobuf::serialize_into(&mut result, &accumulator).ok()?;

        Some(result)
    }
}
