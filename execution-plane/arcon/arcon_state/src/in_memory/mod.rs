// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
use crate::{
    error::*, Aggregator, AggregatorState, Backend, BackendContainer, Config, Handle, Key,
    MapState, Metakey, Reducer, ReducerState, StateType, Value, ValueState, VecState,
};
use smallbox::{space, SmallBox};
use std::{any::Any, collections::HashMap, path::Path};

// we'll store values of size up to 8 * size_of::<usize>() inline
type StoredValue = SmallBox<dyn Any + Send, space::S8>;

#[derive(Debug, Default)]
pub struct InMemory {
    data: HashMap<&'static str, HashMap<Vec<u8>, StoredValue>>,
}

impl InMemory {
    fn get<S: StateType, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<S, IK, N>,
    ) -> &HashMap<Vec<u8>, StoredValue> {
        self.data.get(handle.id).unwrap()
    }
    fn get_mut<S: StateType, IK: Metakey, N: Metakey>(
        &mut self,
        handle: &Handle<S, IK, N>,
    ) -> &mut HashMap<Vec<u8>, StoredValue> {
        self.data.get_mut(handle.id).unwrap()
    }
}

impl Backend for InMemory {
    fn restore_or_create(_config: &Config, _id: String) -> Result<BackendContainer<Self>> {
        Ok(BackendContainer::new(Self::default()))
    }

    fn create(_path: &Path) -> Result<BackendContainer<Self>>
    where
        Self: Sized,
    {
        Ok(BackendContainer::new(Self::default()))
    }

    fn restore(_live_path: &Path, _checkpoint_path: &Path) -> Result<BackendContainer<Self>>
    where
        Self: Sized,
    {
        Ok(BackendContainer::new(Self::default()))
    }

    fn was_restored(&self) -> bool {
        false
    }

    fn checkpoint(&self, _checkpoint_path: &Path) -> Result<()> {
        Ok(())
    }

    // region handle activators
    fn register_value_handle<'s, T: Value, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<ValueState<T>, IK, N>,
    ) {
        self.data.entry(handle.id).or_default();
        handle.registered = true;
    }

    fn register_map_handle<'s, K: Key, V: Value, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<MapState<K, V>, IK, N>,
    ) {
        self.data.entry(handle.id).or_default();
        handle.registered = true;
    }

    fn register_vec_handle<'s, T: Value, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<VecState<T>, IK, N>,
    ) {
        self.data.entry(handle.id).or_default();
        handle.registered = true;
    }

    fn register_reducer_handle<'s, T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<ReducerState<T, F>, IK, N>,
    ) {
        self.data.entry(handle.id).or_default();
        handle.registered = true;
    }

    fn register_aggregator_handle<'s, A: Aggregator, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<AggregatorState<A>, IK, N>,
    ) {
        self.data.entry(handle.id).or_default();
        handle.registered = true;
    }
    // endregion
}

mod aggregator_ops;
mod map_ops;
mod reducer_ops;
mod value_ops;
mod vec_ops;

#[cfg(test)]
pub mod tests {
    use super::*;
    common_state_tests!(InMemory::restore_or_create(&Default::default(), "test".into()).unwrap());
}
