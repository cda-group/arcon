use crate::{
    error::*, Backend, Handle, Metakey, Metered, Reducer, ReducerOps, ReducerState, Value,
};

impl<B: Backend> ReducerOps for Metered<B> {
    measure_delegated! { ReducerOps:
        fn reducer_clear<T: Value, F: Reducer::<T>, IK: Metakey, N: Metakey>(
            &mut self,
            handle: &Handle<ReducerState<T, F>, IK, N>,
        ) -> Result<()>;

        fn reducer_get<T: Value, F: Reducer::<T>, IK: Metakey, N: Metakey>(
            &self,
            handle: &Handle<ReducerState<T, F>, IK, N>,
        ) -> Result<Option<T>>;

        fn reducer_reduce<T: Value, F: Reducer::<T>, IK: Metakey, N: Metakey>(
            &mut self,
            handle: &Handle<ReducerState<T, F>, IK, N>,
            value: T,
        ) -> Result<()>;
    }
}
