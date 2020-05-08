use crate::{
    error::*, handles::BoxedIteratorOfResult, Aggregator, AggregatorState, Handle, Key, MapState,
    Metakey, Reducer, ReducerState, Value, ValueState, VecState,
};

pub trait ValueOps {
    fn value_clear<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<ValueState<T>, IK, N>,
    ) -> Result<()>;

    fn value_get<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ValueState<T>, IK, N>,
    ) -> Result<Option<T>>;

    fn value_set<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<ValueState<T>, IK, N>,
        value: T,
    ) -> Result<Option<T>>;

    fn value_fast_set<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<ValueState<T>, IK, N>,
        value: T,
    ) -> Result<()>;
}

pub trait MapOps {
    fn map_clear<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<MapState<K, V>, IK, N>,
    ) -> Result<()>;

    fn map_get<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<Option<V>>;

    fn map_fast_insert<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<MapState<K, V>, IK, N>,
        key: K,
        value: V,
    ) -> Result<()>;

    fn map_insert<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<MapState<K, V>, IK, N>,
        key: K,
        value: V,
    ) -> Result<Option<V>>;

    fn map_insert_all<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<MapState<K, V>, IK, N>,
        key_value_pairs: impl IntoIterator<Item = (K, V)>,
    ) -> Result<()>;

    fn map_remove<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<Option<V>>;

    fn map_fast_remove<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<()>;

    fn map_contains<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<bool>;

    fn map_iter<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<(K, V)>>;

    fn map_keys<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<K>>;

    fn map_values<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<V>>;

    fn map_len<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<usize>;

    fn map_is_empty<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<bool>;
}

pub trait VecOps {
    fn vec_clear<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<VecState<T>, IK, N>,
    ) -> Result<()>;
    fn vec_append<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<VecState<T>, IK, N>,
        value: T,
    ) -> Result<()>;
    fn vec_get<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<Vec<T>>;
    fn vec_iter<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<T>>;
    fn vec_set<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<VecState<T>, IK, N>,
        value: Vec<T>,
    ) -> Result<()>;
    fn vec_add_all<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<VecState<T>, IK, N>,
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

pub trait ReducerOps {
    fn reducer_clear<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<ReducerState<T, F>, IK, N>,
    ) -> Result<()>;
    fn reducer_get<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ReducerState<T, F>, IK, N>,
    ) -> Result<Option<T>>;
    fn reducer_reduce<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<ReducerState<T, F>, IK, N>,
        value: T,
    ) -> Result<()>;
}

pub trait AggregatorOps {
    fn aggregator_clear<A: Aggregator, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<AggregatorState<A>, IK, N>,
    ) -> Result<()>;
    fn aggregator_get<A: Aggregator, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<AggregatorState<A>, IK, N>,
    ) -> Result<A::Result>;
    fn aggregator_aggregate<A: Aggregator, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &mut Handle<AggregatorState<A>, IK, N>,
        value: A::Input,
    ) -> Result<()>;
}
