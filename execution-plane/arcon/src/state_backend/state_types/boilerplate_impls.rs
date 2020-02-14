// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

// this file has all kinds of simple delegating boilerplate impls
// TODO: maybe try to generate it using macros or build.rs?

use crate::state_backend::{state_types::*, StateBackend};
use error::ArconResult;
use std::ops::{Deref, DerefMut};

impl<SB, IK, N, C> State<dyn StateBackend, IK, N> for WithDynamicBackend<SB, C>
where
    C: State<SB, IK, N>,
    SB: StateBackend + Sized,
{
    fn clear(&self, backend: &mut dyn StateBackend) -> ArconResult<()> {
        self.0.clear(backend.downcast_mut()?)
    }

    fn get_current_key(&self) -> ArconResult<&IK> {
        self.0.get_current_key()
    }
    fn set_current_key(&mut self, new_key: IK) -> ArconResult<()> {
        self.0.set_current_key(new_key)
    }

    fn get_current_namespace(&self) -> ArconResult<&N> {
        self.0.get_current_namespace()
    }
    fn set_current_namespace(&mut self, new_namespace: N) -> ArconResult<()> {
        self.0.set_current_namespace(new_namespace)
    }
}

macro_rules! impl_state_for_boxed_with_dyn_backend {
    ($t: ident < _ $(, $rest: ident)* >) => {
        impl<$($rest),*> State<dyn StateBackend, IK, N> for Box<dyn $t<dyn StateBackend, $($rest),*>> {
            fn clear(&self, backend: &mut dyn StateBackend) -> ArconResult<()> {
                (*self).deref().clear(backend)
            }
            fn get_current_key(&self) -> ArconResult<&IK> {
                (*self).deref().get_current_key()
            }
            fn set_current_key(&mut self, new_key: IK) -> ArconResult<()> {
                (*self).deref_mut().set_current_key(new_key)
            }
            fn get_current_namespace(&self) -> ArconResult<&N> {
                (*self).deref().get_current_namespace()
            }
            fn set_current_namespace(&mut self, new_namespace: N) -> ArconResult<()> {
                (*self).deref_mut().set_current_namespace(new_namespace)
            }
        }
    };
}

impl_state_for_boxed_with_dyn_backend!(State<_, IK, N>);

impl<SB, IK, N, IN, OUT, C> AppendingState<dyn StateBackend, IK, N, IN, OUT>
    for WithDynamicBackend<SB, C>
where
    C: AppendingState<SB, IK, N, IN, OUT>,
    SB: StateBackend + Sized,
{
    fn get(&self, backend: &dyn StateBackend) -> ArconResult<OUT> {
        self.0.get(backend.downcast_ref()?)
    }
    fn append(&self, backend: &mut dyn StateBackend, value: IN) -> ArconResult<()> {
        self.0.append(backend.downcast_mut()?, value)
    }
}

impl_state_for_boxed_with_dyn_backend!(AppendingState<_, IK, N, IN, OUT>);

impl<SB, IK, N, IN, OUT, C> MergingState<dyn StateBackend, IK, N, IN, OUT>
    for WithDynamicBackend<SB, C>
where
    C: MergingState<SB, IK, N, IN, OUT>,
    SB: StateBackend + Sized,
{
}

impl_state_for_boxed_with_dyn_backend!(MergingState<_, IK, N, IN, OUT>);

impl<SB, IK, N, T, C> ValueState<dyn StateBackend, IK, N, T> for WithDynamicBackend<SB, C>
where
    C: ValueState<SB, IK, N, T>,
    SB: StateBackend + Sized,
{
    fn get(&self, backend: &dyn StateBackend) -> ArconResult<T> {
        self.0.get(backend.downcast_ref()?)
    }

    fn set(&self, backend: &mut dyn StateBackend, new_value: T) -> ArconResult<()> {
        self.0.set(backend.downcast_mut()?, new_value)
    }
}

impl_state_for_boxed_with_dyn_backend!(ValueState<_, IK, N, T>);

impl<IK, N, T> ValueState<dyn StateBackend, IK, N, T>
    for Box<dyn ValueState<dyn StateBackend, IK, N, T>>
{
    fn get(&self, backend: &dyn StateBackend) -> ArconResult<T> {
        (*self).deref().get(backend)
    }

    fn set(&self, backend: &mut dyn StateBackend, new_value: T) -> ArconResult<()> {
        (*self).deref().set(backend, new_value)
    }
}

impl<SB, IK, N, K, V, C> MapState<dyn StateBackend, IK, N, K, V> for WithDynamicBackend<SB, C>
where
    C: MapState<SB, IK, N, K, V>,
    SB: StateBackend + Sized,
{
    fn get(&self, backend: &dyn StateBackend, key: &K) -> ArconResult<V> {
        self.0.get(backend.downcast_ref()?, key)
    }
    fn put(&self, backend: &mut dyn StateBackend, key: K, value: V) -> ArconResult<()> {
        self.0.put(backend.downcast_mut()?, key, value)
    }

    fn put_all_dyn(
        &self,
        backend: &mut dyn StateBackend,
        key_value_pairs: &mut dyn Iterator<Item = (K, V)>,
    ) -> ArconResult<()> {
        self.0.put_all_dyn(backend.downcast_mut()?, key_value_pairs)
    }
    fn put_all(
        &self,
        backend: &mut dyn StateBackend,
        key_value_pairs: impl IntoIterator<Item = (K, V)>,
    ) -> ArconResult<()>
    where
        Self: Sized,
    {
        self.0.put_all(backend.downcast_mut()?, key_value_pairs)
    }

    fn remove(&self, backend: &mut dyn StateBackend, key: &K) -> ArconResult<()> {
        self.0.remove(backend.downcast_mut()?, key)
    }
    fn contains(&self, backend: &dyn StateBackend, key: &K) -> ArconResult<bool> {
        self.0.contains(backend.downcast_ref()?, key)
    }

    fn iter<'a>(
        &self,
        backend: &'a dyn StateBackend,
    ) -> ArconResult<Box<dyn Iterator<Item = (K, V)> + 'a>> {
        self.0.iter(backend.downcast_ref()?)
    }
    fn keys<'a>(
        &self,
        backend: &'a dyn StateBackend,
    ) -> ArconResult<Box<dyn Iterator<Item = K> + 'a>> {
        self.0.keys(backend.downcast_ref()?)
    }
    fn values<'a>(
        &self,
        backend: &'a dyn StateBackend,
    ) -> ArconResult<Box<dyn Iterator<Item = V> + 'a>> {
        self.0.values(backend.downcast_ref()?)
    }

    fn is_empty(&self, backend: &dyn StateBackend) -> ArconResult<bool> {
        self.0.is_empty(backend.downcast_ref()?)
    }
}

impl_state_for_boxed_with_dyn_backend!(MapState<_, IK, N, K, V>);

impl<IK, N, K, V> MapState<dyn StateBackend, IK, N, K, V>
    for Box<dyn MapState<dyn StateBackend, IK, N, K, V>>
{
    fn get(&self, backend: &dyn StateBackend, key: &K) -> ArconResult<V> {
        (*self).deref().get(backend, key)
    }
    fn put(&self, backend: &mut dyn StateBackend, key: K, value: V) -> ArconResult<()> {
        (*self).deref().put(backend, key, value)
    }

    fn put_all_dyn(
        &self,
        backend: &mut dyn StateBackend,
        key_value_pairs: &mut dyn Iterator<Item = (K, V)>,
    ) -> ArconResult<()> {
        (*self).deref().put_all_dyn(backend, key_value_pairs)
    }
    fn put_all(
        &self,
        backend: &mut dyn StateBackend,
        key_value_pairs: impl IntoIterator<Item = (K, V)>,
    ) -> ArconResult<()>
    where
        Self: Sized,
    {
        // we fall back to put_all_dyn, because put_all has Self: Sized bound
        (*self)
            .deref()
            .put_all_dyn(backend, &mut key_value_pairs.into_iter())
    }

    fn remove(&self, backend: &mut dyn StateBackend, key: &K) -> ArconResult<()> {
        (*self).deref().remove(backend, key)
    }
    fn contains(&self, backend: &dyn StateBackend, key: &K) -> ArconResult<bool> {
        (*self).deref().contains(backend, key)
    }

    fn iter<'a>(
        &self,
        backend: &'a dyn StateBackend,
    ) -> ArconResult<Box<dyn Iterator<Item = (K, V)> + 'a>> {
        (*self).deref().iter(backend)
    }
    fn keys<'a>(
        &self,
        backend: &'a dyn StateBackend,
    ) -> ArconResult<Box<dyn Iterator<Item = K> + 'a>> {
        (*self).deref().keys(backend)
    }
    fn values<'a>(
        &self,
        backend: &'a dyn StateBackend,
    ) -> ArconResult<Box<dyn Iterator<Item = V> + 'a>> {
        (*self).deref().values(backend)
    }

    fn is_empty(&self, backend: &dyn StateBackend) -> ArconResult<bool> {
        (*self).deref().is_empty(backend)
    }
}

impl<SB, IK, N, T, C> VecState<dyn StateBackend, IK, N, T> for WithDynamicBackend<SB, C>
where
    C: VecState<SB, IK, N, T>,
    SB: StateBackend + Sized,
{
    fn set(&self, backend: &mut dyn StateBackend, value: Vec<T>) -> ArconResult<()> {
        self.0.set(backend.downcast_mut()?, value)
    }

    fn add_all(
        &self,
        backend: &mut dyn StateBackend,
        values: impl IntoIterator<Item = T>,
    ) -> ArconResult<()>
    where
        Self: Sized,
    {
        self.0.add_all(backend.downcast_mut()?, values)
    }

    fn add_all_dyn(
        &self,
        backend: &mut dyn StateBackend,
        values: &mut dyn Iterator<Item = T>,
    ) -> ArconResult<()> {
        self.0.add_all_dyn(backend.downcast_mut()?, values)
    }
}

impl_state_for_boxed_with_dyn_backend!(VecState<_, IK, N, T>);

impl<IK, N, T> AppendingState<dyn StateBackend, IK, N, T, Vec<T>>
    for Box<dyn VecState<dyn StateBackend, IK, N, T>>
{
    fn get(&self, backend: &dyn StateBackend) -> ArconResult<Vec<T>> {
        (*self).deref().get(backend)
    }

    fn append(&self, backend: &mut dyn StateBackend, value: T) -> ArconResult<()> {
        (*self).deref().append(backend, value)
    }
}

impl<IK, N, T> MergingState<dyn StateBackend, IK, N, T, Vec<T>>
    for Box<dyn VecState<dyn StateBackend, IK, N, T>>
{
}

impl<IK, N, T> VecState<dyn StateBackend, IK, N, T>
    for Box<dyn VecState<dyn StateBackend, IK, N, T>>
{
    fn set(&self, backend: &mut dyn StateBackend, value: Vec<T>) -> ArconResult<()> {
        (*self).deref().set(backend, value)
    }

    fn add_all(
        &self,
        backend: &mut dyn StateBackend,
        values: impl IntoIterator<Item = T>,
    ) -> ArconResult<()>
    where
        Self: Sized,
    {
        // we fall back to add_all_dyn, because add_all has Self: Sized bound
        (*self)
            .deref()
            .add_all_dyn(backend, &mut values.into_iter())
    }

    fn add_all_dyn(
        &self,
        backend: &mut dyn StateBackend,
        values: &mut dyn Iterator<Item = T>,
    ) -> ArconResult<()> {
        (*self).deref().add_all_dyn(backend, values)
    }
}

impl<SB, IK, N, T, C> ReducingState<dyn StateBackend, IK, N, T> for WithDynamicBackend<SB, C>
where
    C: ReducingState<SB, IK, N, T>,
    SB: StateBackend + Sized,
{
}

impl_state_for_boxed_with_dyn_backend!(ReducingState<_, IK, N, T>);

impl<IK, N, T> AppendingState<dyn StateBackend, IK, N, T, T>
    for Box<dyn ReducingState<dyn StateBackend, IK, N, T>>
{
    fn get(&self, backend: &dyn StateBackend) -> ArconResult<T> {
        (*self).deref().get(backend)
    }

    fn append(&self, backend: &mut dyn StateBackend, value: T) -> ArconResult<()> {
        (*self).deref().append(backend, value)
    }
}

impl<IK, N, T> MergingState<dyn StateBackend, IK, N, T, T>
    for Box<dyn ReducingState<dyn StateBackend, IK, N, T>>
{
}

impl<IK, N, T> ReducingState<dyn StateBackend, IK, N, T>
    for Box<dyn ReducingState<dyn StateBackend, IK, N, T>>
{
}

impl<SB, IK, N, IN, OUT, C> AggregatingState<dyn StateBackend, IK, N, IN, OUT>
    for WithDynamicBackend<SB, C>
where
    C: AggregatingState<SB, IK, N, IN, OUT>,
    SB: StateBackend + Sized,
{
}

impl_state_for_boxed_with_dyn_backend!(AggregatingState<_, IK, N, IN, OUT>);

impl<IK, N, IN, OUT> AppendingState<dyn StateBackend, IK, N, IN, OUT>
    for Box<dyn AggregatingState<dyn StateBackend, IK, N, IN, OUT>>
{
    fn get(&self, backend: &dyn StateBackend) -> ArconResult<OUT> {
        (*self).deref().get(backend)
    }

    fn append(&self, backend: &mut dyn StateBackend, value: IN) -> ArconResult<()> {
        (*self).deref().append(backend, value)
    }
}

impl<IK, N, IN, OUT> MergingState<dyn StateBackend, IK, N, IN, OUT>
    for Box<dyn AggregatingState<dyn StateBackend, IK, N, IN, OUT>>
{
}

impl<IK, N, IN, OUT> AggregatingState<dyn StateBackend, IK, N, IN, OUT>
    for Box<dyn AggregatingState<dyn StateBackend, IK, N, IN, OUT>>
{
}
