// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
///! Dummy backend for tests
use super::*;
use crate::error::ArconStateError::DummyImplError;

#[derive(Debug)]
pub struct DummyBackend;

impl Backend for DummyBackend {
    fn restore_or_create(_config: &Config, _id: String) -> Result<Self> {
        Ok(DummyBackend)
    }
    fn checkpoint(&self, _config: &Config) -> Result<()> {
        Ok(())
    }

    fn build_active_value_handle<'s, T: Value, IK: Metakey, N: Metakey>(
        &'s mut self,
        inner: &'s mut Handle<ValueState<T>, IK, N>,
    ) -> ActiveHandle<'s, Self, ValueState<T>, IK, N> {
        ActiveHandle {
            backend: self,
            inner,
        }
    }

    fn build_active_map_handle<'s, K: Key, V: Value, IK: Metakey, N: Metakey>(
        &'s mut self,
        inner: &'s mut Handle<MapState<K, V>, IK, N>,
    ) -> ActiveHandle<'s, Self, MapState<K, V>, IK, N> {
        ActiveHandle {
            backend: self,
            inner,
        }
    }

    fn build_active_vec_handle<'s, T: Value, IK: Metakey, N: Metakey>(
        &'s mut self,
        inner: &'s mut Handle<VecState<T>, IK, N>,
    ) -> ActiveHandle<'s, Self, VecState<T>, IK, N> {
        ActiveHandle {
            backend: self,
            inner,
        }
    }

    fn build_active_reducer_handle<'s, T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &'s mut self,
        inner: &'s mut Handle<ReducerState<T, F>, IK, N>,
    ) -> ActiveHandle<'s, Self, ReducerState<T, F>, IK, N> {
        ActiveHandle {
            backend: self,
            inner,
        }
    }

    fn build_active_aggregator_handle<'s, A: Aggregator, IK: Metakey, N: Metakey>(
        &'s mut self,
        inner: &'s mut Handle<AggregatorState<A>, IK, N>,
    ) -> ActiveHandle<'s, Self, AggregatorState<A>, IK, N> {
        ActiveHandle {
            backend: self,
            inner,
        }
    }

    fn value_clear<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        _handle: &mut Handle<ValueState<T>, IK, N>,
    ) -> Result<()> {
        Err(DummyImplError)
    }

    fn value_get<T: Value, IK: Metakey, N: Metakey>(
        &self,
        _handle: &Handle<ValueState<T>, IK, N>,
    ) -> Result<Option<T>> {
        Err(DummyImplError)
    }

    fn value_set<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        _handle: &mut Handle<ValueState<T>, IK, N>,
        _value: T,
    ) -> Result<Option<T>> {
        Err(DummyImplError)
    }

    fn value_fast_set<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        _handle: &mut Handle<ValueState<T>, IK, N>,
        _value: T,
    ) -> Result<()> {
        Err(DummyImplError)
    }

    fn map_clear<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        _handle: &mut Handle<MapState<K, V>, IK, N>,
    ) -> Result<()> {
        Err(DummyImplError)
    }

    fn map_get<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        _handle: &Handle<MapState<K, V>, IK, N>,
        _key: &K,
    ) -> Result<Option<V>> {
        Err(DummyImplError)
    }

    fn map_fast_insert<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        _handle: &mut Handle<MapState<K, V>, IK, N>,
        _key: K,
        _value: V,
    ) -> Result<()> {
        Err(DummyImplError)
    }

    fn map_insert<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        _handle: &mut Handle<MapState<K, V>, IK, N>,
        _key: K,
        _value: V,
    ) -> Result<Option<V>> {
        Err(DummyImplError)
    }

    fn map_insert_all<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        _handle: &mut Handle<MapState<K, V>, IK, N>,
        _key_value_pairs: impl IntoIterator<Item = (K, V)>,
    ) -> Result<()> {
        Err(DummyImplError)
    }

    fn map_remove<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        _handle: &mut Handle<MapState<K, V>, IK, N>,
        _key: &K,
    ) -> Result<Option<V>> {
        Err(DummyImplError)
    }

    fn map_fast_remove<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &mut self,
        _handle: &mut Handle<MapState<K, V>, IK, N>,
        _key: &K,
    ) -> Result<()> {
        Err(DummyImplError)
    }

    fn map_contains<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        _handle: &Handle<MapState<K, V>, IK, N>,
        _key: &K,
    ) -> Result<bool> {
        Err(DummyImplError)
    }

    fn map_iter<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        _handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<(K, V)>> {
        Err(DummyImplError)
    }

    fn map_keys<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        _handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<K>> {
        Err(DummyImplError)
    }

    fn map_values<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        _handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<V>> {
        Err(DummyImplError)
    }

    fn map_len<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        _handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<usize> {
        Err(DummyImplError)
    }

    fn map_is_empty<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        _handle: &Handle<MapState<K, V>, IK, N>,
    ) -> Result<bool> {
        Err(DummyImplError)
    }

    fn vec_clear<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        _handle: &mut Handle<VecState<T>, IK, N>,
    ) -> Result<()> {
        Err(DummyImplError)
    }

    fn vec_append<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        _handle: &mut Handle<VecState<T>, IK, N>,
        _value: T,
    ) -> Result<()> {
        Err(DummyImplError)
    }

    fn vec_get<T: Value, IK: Metakey, N: Metakey>(
        &self,
        _handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<Vec<T>> {
        Err(DummyImplError)
    }

    fn vec_iter<T: Value, IK: Metakey, N: Metakey>(
        &self,
        _handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<BoxedIteratorOfResult<'_, T>> {
        Err(DummyImplError)
    }

    fn vec_set<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        _handle: &mut Handle<VecState<T>, IK, N>,
        _value: Vec<T>,
    ) -> Result<()> {
        Err(DummyImplError)
    }

    fn vec_add_all<T: Value, IK: Metakey, N: Metakey>(
        &mut self,
        _handle: &mut Handle<VecState<T>, IK, N>,
        _value: impl IntoIterator<Item = T>,
    ) -> Result<()> {
        Err(DummyImplError)
    }

    fn vec_len<T: Value, IK: Metakey, N: Metakey>(
        &self,
        _handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<usize> {
        Err(DummyImplError)
    }

    fn vec_is_empty<T: Value, IK: Metakey, N: Metakey>(
        &self,
        _handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<bool> {
        Err(DummyImplError)
    }

    fn reducer_clear<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &mut self,
        _handle: &mut Handle<ReducerState<T, F>, IK, N>,
    ) -> Result<()> {
        Err(DummyImplError)
    }

    fn reducer_get<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &self,
        _handle: &Handle<ReducerState<T, F>, IK, N>,
    ) -> Result<Option<T>> {
        Err(DummyImplError)
    }

    fn reducer_reduce<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &mut self,
        _handle: &mut Handle<ReducerState<T, F>, IK, N>,
        _value: T,
    ) -> Result<()> {
        Err(DummyImplError)
    }

    fn aggregator_clear<A: Aggregator, IK: Metakey, N: Metakey>(
        &mut self,
        _handle: &mut Handle<AggregatorState<A>, IK, N>,
    ) -> Result<()> {
        Err(DummyImplError)
    }

    fn aggregator_get<A: Aggregator, IK: Metakey, N: Metakey>(
        &self,
        _handle: &Handle<AggregatorState<A>, IK, N>,
    ) -> Result<A::Result> {
        Err(DummyImplError)
    }

    fn aggregator_aggregate<A: Aggregator, IK: Metakey, N: Metakey>(
        &mut self,
        _handle: &mut Handle<AggregatorState<A>, IK, N>,
        _value: A::Input,
    ) -> Result<()> {
        Err(DummyImplError)
    }
}
