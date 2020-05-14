use crate::{
    error::*, handles::BoxedIteratorOfResult, Backend, Handle, Metakey, Metered, Value, VecOps,
    VecState,
};
use std::iter;

impl<B: Backend> VecOps for Metered<B> {
    measure_delegated! { VecOps:
        fn vec_clear<T: Value, IK: Metakey, N: Metakey>(
            &mut self,
            handle: &Handle<VecState<T>, IK, N>,
        ) -> Result<()>;

        fn vec_append<T: Value, IK: Metakey, N: Metakey>(
            &mut self,
            handle: &Handle<VecState<T>, IK, N>,
            value: T,
        ) -> Result<()>;

        fn vec_get<T: Value, IK: Metakey, N: Metakey>(
            &self,
            handle: &Handle<VecState<T>, IK, N>,
        ) -> Result<Vec<T>>;

        fn vec_set<T: Value, IK: Metakey, N: Metakey>(
            &mut self,
            handle: &Handle<VecState<T>, IK, N>,
            value: Vec<T>,
        ) -> Result<()>;

        fn vec_add_all<T: Value, IK: Metakey, N: Metakey>(
            &mut self,
            handle: &Handle<VecState<T>, IK, N>,
            values: impl IntoIterator<Item = T>,
        ) -> Result<()>;

        fn vec_len<T: Value, IK: Metakey, N: Metakey>(
            &self,
            handle: &Handle<VecState<T>, IK, N>,
        ) -> Result<usize>;

        fn vec_is_empty<T: Value, IK: Metakey, N: Metakey>(
            &self,
            handle: &Handle<VecState<T>, IK, N>,
        ) -> Result<bool>;
    }

    fn vec_iter<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, T>> {
        let mut iter = self.measure("VecOps::vec_iter", |backend| backend.vec_iter(handle))?;
        let iter = iter::from_fn(move || self.measure("VecOps::vec_iter::next", |_| iter.next()));
        Ok(Box::new(iter))
    }
}
