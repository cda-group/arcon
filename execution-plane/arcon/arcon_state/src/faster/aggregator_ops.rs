use crate::{
    error::*, serialization::protobuf, Aggregator, AggregatorOps, AggregatorState, Faster, Handle,
    Metakey,
};

pub(crate) const ACCUMULATOR_MARKER: u8 = 0xAC;
pub(crate) const VALUE_MARKER: u8 = 0x00;

impl AggregatorOps for Faster {
    fn aggregator_clear<A: Aggregator, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<AggregatorState<A>, IK, N>,
    ) -> Result<()> {
        let key = handle.serialize_id_and_metakeys()?;
        self.remove(&key)?;
        Ok(())
    }

    fn aggregator_get<A: Aggregator, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<AggregatorState<A>, IK, N>,
    ) -> Result<<A as Aggregator>::Result> {
        let key = handle.serialize_id_and_metakeys()?;

        if let Some(serialized) = self.get_agg(&key)? {
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
        &mut self,
        handle: &mut Handle<AggregatorState<A>, IK, N>,
        value: <A as Aggregator>::Input,
    ) -> Result<()> {
        let key = handle.serialize_id_and_metakeys()?;
        let mut serialized = Vec::with_capacity(1 + protobuf::size_hint(&value).unwrap_or(0));
        serialized.push(VALUE_MARKER);
        protobuf::serialize_into(&mut serialized, &value)?;

        // See the make_aggregate_fn function in this module. Its result is set as the
        // merging operator for this state.
        self.aggregate(&key, serialized, handle.id)
    }
}

pub fn make_aggregate_fn<A>(aggregator: A) -> Box<dyn Fn(&[u8], &[u8]) -> Vec<u8> + Send>
where
    A: Aggregator,
{
    Box::new(move |old, new| {
        let mut accumulator: A::Accumulator = {
            match old {
                [ACCUMULATOR_MARKER, accumulator_bytes @ ..] => {
                    protobuf::deserialize(accumulator_bytes)
                        .expect("accumulator deserialization error")
                }
                [VALUE_MARKER, value_bytes @ ..] => {
                    let value =
                        protobuf::deserialize(value_bytes).expect("value deserialization error");
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
                let second_acc = protobuf::deserialize(accumulator_bytes)
                    .expect("accumulator deserialization error");

                accumulator = aggregator.merge_accumulators(accumulator, second_acc);
            }
            [VALUE_MARKER, value_bytes @ ..] => {
                let value =
                    protobuf::deserialize(value_bytes).expect("value deserialization error");

                aggregator.add(&mut accumulator, value);
            }
            _ => {
                panic!("unknown operand in aggregate merge operator");
            }
        }

        let mut result = Vec::with_capacity(1 + protobuf::size_hint(&accumulator).unwrap_or(0));
        result.push(ACCUMULATOR_MARKER);

        protobuf::serialize_into(&mut result, &accumulator)
            .expect("accumulator serialization error");

        result
    })
}
