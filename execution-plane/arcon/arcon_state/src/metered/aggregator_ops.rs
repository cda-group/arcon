use crate::{
    error::*, Aggregator, AggregatorOps, AggregatorState, Backend, Handle, Metakey, Metered,
};

impl<B: Backend> AggregatorOps for Metered<B> {
    measure_delegated! { AggregatorOps:
        fn aggregator_clear<A: Aggregator, IK: Metakey, N: Metakey>(
            &mut self,
            handle: &mut Handle<AggregatorState<A>, IK, N>,
        ) -> Result<()>;

        fn aggregator_get<A: Aggregator, IK: Metakey, N: Metakey>(
            &self,
            handle: &Handle<AggregatorState<A>, IK, N>,
        ) -> Result<<A as Aggregator>::Result>;

        fn aggregator_aggregate<A: Aggregator, IK: Metakey, N: Metakey>(
            &mut self,
            handle: &mut Handle<AggregatorState<A>, IK, N>,
            value: <A as Aggregator>::Input,
        ) -> Result<()>;
    }
}
