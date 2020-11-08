// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::backend::ops::{AggregatorOps, MapOps, ReducerOps, ValueOps, VecOps};
use std::path::Path;

use crate::{
    error::*, handles::BoxedIteratorOfResult, Aggregator, AggregatorState, Backend, Handle, Key,
    MapState, Metakey, Reducer, ReducerState, Value, ValueState, VecState,
};

/// `BackendNever` may be used as an empty Backend type.
pub struct BackendNever {}

impl Backend for BackendNever {
    #[allow(unused_variables)]
    fn create(live_path: &Path) -> Result<Self>
    where
        Self: Sized,
    {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn restore(live_path: &Path, checkpoint_path: &Path) -> Result<Self>
    where
        Self: Sized,
    {
        unreachable!();
    }

    fn was_restored(&self) -> bool {
        unreachable!();
    }

    fn checkpoint(&self, _checkpoint_path: &Path) -> Result<()> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn register_value_handle<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &mut Handle<ValueState<T>, IK, N>,
    ) {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn register_map_handle<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &mut Handle<MapState<K, V>, IK, N>,
    ) {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn register_vec_handle<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &mut Handle<VecState<T>, IK, N>,
    ) {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn register_reducer_handle<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &self,
        handle: &mut Handle<ReducerState<T, F>, IK, N>,
    ) {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn register_aggregator_handle<A: Aggregator, IK: Metakey, N: Metakey>(
        &self,
        handle: &mut Handle<AggregatorState<A>, IK, N>,
    ) {
        unreachable!();
    }
}

impl ValueOps for BackendNever {
    #[allow(unused_variables)]
    fn value_clear<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ValueState<T>, IK, N>,
    ) -> Result<()> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn value_get<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ValueState<T>, IK, N>,
    ) -> Result<Option<T>> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn value_set<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ValueState<T>, IK, N>,
        value: T,
    ) -> Result<Option<T>> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn value_fast_set<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ValueState<T>, IK, N>,
        value: T,
    ) -> Result<()> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn value_fast_set_by_ref<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ValueState<T>, IK, N>,
        value: &T,
    ) -> Result<()> {
        unreachable!();
    }
}

impl VecOps for BackendNever {
    #[allow(unused_variables)]
    fn vec_clear<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<()> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn vec_append<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
        value: T,
    ) -> Result<()> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn vec_get<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<Vec<T>> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn vec_iter<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, T>> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn vec_set<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
        value: Vec<T>,
    ) -> Result<()> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn vec_add_all<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
        values: impl IntoIterator<Item = T>,
    ) -> Result<()> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn vec_len<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<usize> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn vec_is_empty<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<bool> {
        unreachable!();
    }
}

impl MapOps for BackendNever {
    #[allow(unused_variables)]
    fn map_clear<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<()> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn map_get<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<Option<V>> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn map_fast_insert<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: K,
        value: V,
    ) -> Result<()> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn map_fast_insert_by_ref<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
        value: &V,
    ) -> Result<()> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn map_insert<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: K,
        value: V,
    ) -> Result<Option<V>> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn map_insert_all<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key_value_pairs: impl IntoIterator<Item = (K, V)>,
    ) -> Result<()> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn map_insert_all_by_ref<'a, K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key_value_pairs: impl IntoIterator<Item = (&'a K, &'a V)>,
    ) -> Result<()> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn map_remove<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<Option<V>> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn map_fast_remove<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<()> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn map_contains<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
        key: &K,
    ) -> Result<bool> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn map_iter<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, (K, V)>> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn map_keys<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, K>> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn map_values<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, V>> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn map_len<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<usize> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn map_is_empty<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<bool> {
        unreachable!();
    }
}

impl ReducerOps for BackendNever {
    #[allow(unused_variables)]
    fn reducer_clear<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ReducerState<T, F>, IK, N>,
    ) -> Result<()> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn reducer_get<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ReducerState<T, F>, IK, N>,
    ) -> Result<Option<T>> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn reducer_reduce<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<ReducerState<T, F>, IK, N>,
        value: T,
    ) -> Result<()> {
        unreachable!();
    }
}

impl AggregatorOps for BackendNever {
    #[allow(unused_variables)]
    fn aggregator_clear<A: Aggregator, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<AggregatorState<A>, IK, N>,
    ) -> Result<()> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn aggregator_get<A: Aggregator, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<AggregatorState<A>, IK, N>,
    ) -> Result<<A as Aggregator>::Result> {
        unreachable!();
    }

    #[allow(unused_variables)]
    fn aggregator_aggregate<A: Aggregator, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<AggregatorState<A>, IK, N>,
        value: <A as Aggregator>::Input,
    ) -> Result<()> {
        unreachable!();
    }
}
