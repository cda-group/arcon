use crate::{error::*, Backend, Handle, Metakey, Metered, Value, ValueOps, ValueState};

impl<B: Backend> ValueOps for Metered<B> {
    measure_delegated! { ValueOps:
        fn value_clear<T: Value, IK: Metakey, N: Metakey>(
            &mut self,
            handle: &Handle<ValueState<T>, IK, N>,
        ) -> Result<()>;

        fn value_get<T: Value, IK: Metakey, N: Metakey>(
            &self,
            handle: &Handle<ValueState<T>, IK, N>,
        ) -> Result<Option<T>>;

        fn value_set<T: Value, IK: Metakey, N: Metakey>(
            &mut self,
            handle: &Handle<ValueState<T>, IK, N>,
            value: T,
        ) -> Result<Option<T>>;

        fn value_fast_set<T: Value, IK: Metakey, N: Metakey>(
            &mut self,
            handle: &Handle<ValueState<T>, IK, N>,
            value: T,
        ) -> Result<()>;
    }
}
