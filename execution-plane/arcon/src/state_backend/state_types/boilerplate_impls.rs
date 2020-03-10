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
    #[inline]
    fn clear(&self, backend: &mut dyn StateBackend) -> ArconResult<()> {
        self.0.clear(backend.downcast_mut()?)
    }

    #[inline]
    fn get_current_key(&self) -> ArconResult<&IK> {
        self.0.get_current_key()
    }
    #[inline]
    fn set_current_key(&mut self, new_key: IK) -> ArconResult<()> {
        self.0.set_current_key(new_key)
    }

    #[inline]
    fn get_current_namespace(&self) -> ArconResult<&N> {
        self.0.get_current_namespace()
    }
    #[inline]
    fn set_current_namespace(&mut self, new_namespace: N) -> ArconResult<()> {
        self.0.set_current_namespace(new_namespace)
    }
}

macro_rules! impl_state_for_boxed_with_dyn_backend {
    ($t: ident < _ $(, $rest: ident)* >) => {
        impl<$($rest),*> State<dyn StateBackend, IK, N> for Box<dyn $t<dyn StateBackend, $($rest),*> + Send + Sync + 'static> {
            #[inline]
            fn clear(&self, backend: &mut dyn StateBackend) -> ArconResult<()> {
                (*self).deref().clear(backend)
            }
            #[inline]
            fn get_current_key(&self) -> ArconResult<&IK> {
                (*self).deref().get_current_key()
            }
            #[inline]
            fn set_current_key(&mut self, new_key: IK) -> ArconResult<()> {
                (*self).deref_mut().set_current_key(new_key)
            }
            #[inline]
            fn get_current_namespace(&self) -> ArconResult<&N> {
                (*self).deref().get_current_namespace()
            }
            #[inline]
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
    #[inline]
    fn get(&self, backend: &dyn StateBackend) -> ArconResult<OUT> {
        self.0.get(backend.downcast_ref()?)
    }
    #[inline]
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
    #[inline]
    fn get(&self, backend: &dyn StateBackend) -> ArconResult<Option<T>> {
        self.0.get(backend.downcast_ref()?)
    }

    #[inline]
    fn set(&self, backend: &mut dyn StateBackend, new_value: T) -> ArconResult<()> {
        self.0.set(backend.downcast_mut()?, new_value)
    }
}

impl_state_for_boxed_with_dyn_backend!(ValueState<_, IK, N, T>);

// TODO: these might be unnecessary if I change the bound on _Builder::Type to be a BorrowMut or DerefMut? maybe?
impl<IK, N, T> ValueState<dyn StateBackend, IK, N, T> for BoxedValueState<T, IK, N> {
    #[inline]
    fn get(&self, backend: &dyn StateBackend) -> ArconResult<Option<T>> {
        (*self).deref().get(backend)
    }

    #[inline]
    fn set(&self, backend: &mut dyn StateBackend, new_value: T) -> ArconResult<()> {
        (*self).deref().set(backend, new_value)
    }
}

impl<SB, IK, N, K, V, C> MapState<dyn StateBackend, IK, N, K, V> for WithDynamicBackend<SB, C>
where
    C: MapState<SB, IK, N, K, V>,
    SB: StateBackend + Sized,
{
    #[inline]
    fn get(&self, backend: &dyn StateBackend, key: &K) -> ArconResult<Option<V>> {
        self.0.get(backend.downcast_ref()?, key)
    }
    #[inline]
    fn fast_insert(&self, backend: &mut dyn StateBackend, key: K, value: V) -> ArconResult<()> {
        self.0.fast_insert(backend.downcast_mut()?, key, value)
    }

    #[inline]
    fn insert(&self, backend: &mut dyn StateBackend, key: K, value: V) -> ArconResult<Option<V>> {
        self.0.insert(backend.downcast_mut()?, key, value)
    }

    #[inline]
    fn insert_all_dyn(
        &self,
        backend: &mut dyn StateBackend,
        key_value_pairs: &mut dyn Iterator<Item = (K, V)>,
    ) -> ArconResult<()> {
        self.0
            .insert_all_dyn(backend.downcast_mut()?, key_value_pairs)
    }
    #[inline]
    fn insert_all(
        &self,
        backend: &mut dyn StateBackend,
        key_value_pairs: impl IntoIterator<Item = (K, V)>,
    ) -> ArconResult<()>
    where
        Self: Sized,
    {
        self.0.insert_all(backend.downcast_mut()?, key_value_pairs)
    }

    #[inline]
    fn remove(&self, backend: &mut dyn StateBackend, key: &K) -> ArconResult<()> {
        self.0.remove(backend.downcast_mut()?, key)
    }
    #[inline]
    fn contains(&self, backend: &dyn StateBackend, key: &K) -> ArconResult<bool> {
        self.0.contains(backend.downcast_ref()?, key)
    }

    #[inline]
    fn iter<'a>(
        &self,
        backend: &'a dyn StateBackend,
    ) -> ArconResult<BoxedIteratorOfArconResult<'a, (K, V)>> {
        self.0.iter(backend.downcast_ref()?)
    }
    #[inline]
    fn keys<'a>(
        &self,
        backend: &'a dyn StateBackend,
    ) -> ArconResult<BoxedIteratorOfArconResult<'a, K>> {
        self.0.keys(backend.downcast_ref()?)
    }
    #[inline]
    fn values<'a>(
        &self,
        backend: &'a dyn StateBackend,
    ) -> ArconResult<BoxedIteratorOfArconResult<'a, V>> {
        self.0.values(backend.downcast_ref()?)
    }

    #[inline]
    fn len(&self, backend: &dyn StateBackend) -> ArconResult<usize> {
        self.0.len(backend.downcast_ref()?)
    }

    #[inline]
    fn is_empty(&self, backend: &dyn StateBackend) -> ArconResult<bool> {
        self.0.is_empty(backend.downcast_ref()?)
    }
}

impl_state_for_boxed_with_dyn_backend!(MapState<_, IK, N, K, V>);

impl<IK, N, K, V> MapState<dyn StateBackend, IK, N, K, V> for BoxedMapState<K, V, IK, N> {
    #[inline]
    fn get(&self, backend: &dyn StateBackend, key: &K) -> ArconResult<Option<V>> {
        (*self).deref().get(backend, key)
    }
    #[inline]
    fn fast_insert(&self, backend: &mut dyn StateBackend, key: K, value: V) -> ArconResult<()> {
        (*self).deref().fast_insert(backend, key, value)
    }

    #[inline]
    fn insert(&self, backend: &mut dyn StateBackend, key: K, value: V) -> ArconResult<Option<V>> {
        (*self).deref().insert(backend, key, value)
    }

    #[inline]
    fn insert_all_dyn(
        &self,
        backend: &mut dyn StateBackend,
        key_value_pairs: &mut dyn Iterator<Item = (K, V)>,
    ) -> ArconResult<()> {
        (*self).deref().insert_all_dyn(backend, key_value_pairs)
    }
    #[inline]
    fn insert_all(
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
            .insert_all_dyn(backend, &mut key_value_pairs.into_iter())
    }

    #[inline]
    fn remove(&self, backend: &mut dyn StateBackend, key: &K) -> ArconResult<()> {
        (*self).deref().remove(backend, key)
    }
    #[inline]
    fn contains(&self, backend: &dyn StateBackend, key: &K) -> ArconResult<bool> {
        (*self).deref().contains(backend, key)
    }

    #[inline]
    fn iter<'a>(
        &self,
        backend: &'a dyn StateBackend,
    ) -> ArconResult<BoxedIteratorOfArconResult<'a, (K, V)>> {
        (*self).deref().iter(backend)
    }
    #[inline]
    fn keys<'a>(
        &self,
        backend: &'a dyn StateBackend,
    ) -> ArconResult<BoxedIteratorOfArconResult<'a, K>> {
        (*self).deref().keys(backend)
    }
    #[inline]
    fn values<'a>(
        &self,
        backend: &'a dyn StateBackend,
    ) -> ArconResult<BoxedIteratorOfArconResult<'a, V>> {
        (*self).deref().values(backend)
    }

    #[inline]
    fn len(&self, backend: &dyn StateBackend) -> ArconResult<usize> {
        (*self).deref().len(backend)
    }

    #[inline]
    fn is_empty(&self, backend: &dyn StateBackend) -> ArconResult<bool> {
        (*self).deref().is_empty(backend)
    }
}

impl<SB, IK, N, T, C> VecState<dyn StateBackend, IK, N, T> for WithDynamicBackend<SB, C>
where
    C: VecState<SB, IK, N, T>,
    SB: StateBackend + Sized,
{
    #[inline]
    fn set(&self, backend: &mut dyn StateBackend, value: Vec<T>) -> ArconResult<()> {
        self.0.set(backend.downcast_mut()?, value)
    }

    #[inline]
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

    #[inline]
    fn add_all_dyn(
        &self,
        backend: &mut dyn StateBackend,
        values: &mut dyn Iterator<Item = T>,
    ) -> ArconResult<()> {
        self.0.add_all_dyn(backend.downcast_mut()?, values)
    }

    #[inline]
    fn is_empty(&self, backend: &dyn StateBackend) -> ArconResult<bool> {
        self.0.is_empty(backend.downcast_ref()?)
    }

    fn len(&self, backend: &dyn StateBackend) -> ArconResult<usize> {
        self.0.len(backend.downcast_ref()?)
    }
}

impl_state_for_boxed_with_dyn_backend!(VecState<_, IK, N, T>);

impl<IK, N, T> AppendingState<dyn StateBackend, IK, N, T, Vec<T>> for BoxedVecState<T, IK, N> {
    #[inline]
    fn get(&self, backend: &dyn StateBackend) -> ArconResult<Vec<T>> {
        (*self).deref().get(backend)
    }

    #[inline]
    fn append(&self, backend: &mut dyn StateBackend, value: T) -> ArconResult<()> {
        (*self).deref().append(backend, value)
    }
}

impl<IK, N, T> MergingState<dyn StateBackend, IK, N, T, Vec<T>> for BoxedVecState<T, IK, N> {}

impl<IK, N, T> VecState<dyn StateBackend, IK, N, T> for BoxedVecState<T, IK, N> {
    #[inline]
    fn set(&self, backend: &mut dyn StateBackend, value: Vec<T>) -> ArconResult<()> {
        (*self).deref().set(backend, value)
    }

    #[inline]
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

    #[inline]
    fn add_all_dyn(
        &self,
        backend: &mut dyn StateBackend,
        values: &mut dyn Iterator<Item = T>,
    ) -> ArconResult<()> {
        (*self).deref().add_all_dyn(backend, values)
    }

    #[inline]
    fn is_empty(&self, backend: &dyn StateBackend) -> ArconResult<bool> {
        (*self).deref().is_empty(backend)
    }

    fn len(&self, backend: &dyn StateBackend) -> ArconResult<usize> {
        (*self).deref().len(backend)
    }
}

impl<SB, IK, N, T, C> ReducingState<dyn StateBackend, IK, N, T> for WithDynamicBackend<SB, C>
where
    C: ReducingState<SB, IK, N, T>,
    SB: StateBackend + Sized,
{
}

impl_state_for_boxed_with_dyn_backend!(ReducingState<_, IK, N, T>);

impl<IK, N, T> AppendingState<dyn StateBackend, IK, N, T, Option<T>>
    for BoxedReducingState<T, IK, N>
{
    #[inline]
    fn get(&self, backend: &dyn StateBackend) -> ArconResult<Option<T>> {
        (*self).deref().get(backend)
    }

    #[inline]
    fn append(&self, backend: &mut dyn StateBackend, value: T) -> ArconResult<()> {
        (*self).deref().append(backend, value)
    }
}

impl<IK, N, T> MergingState<dyn StateBackend, IK, N, T, Option<T>>
    for BoxedReducingState<T, IK, N>
{
}

impl<IK, N, T> ReducingState<dyn StateBackend, IK, N, T> for BoxedReducingState<T, IK, N> {}

impl<SB, IK, N, IN, OUT, C> AggregatingState<dyn StateBackend, IK, N, IN, OUT>
    for WithDynamicBackend<SB, C>
where
    C: AggregatingState<SB, IK, N, IN, OUT>,
    SB: StateBackend + Sized,
{
}

impl_state_for_boxed_with_dyn_backend!(AggregatingState<_, IK, N, IN, OUT>);

impl<IK, N, IN, OUT> AppendingState<dyn StateBackend, IK, N, IN, OUT>
    for BoxedAggregatingState<IN, OUT, IK, N>
{
    #[inline]
    fn get(&self, backend: &dyn StateBackend) -> ArconResult<OUT> {
        (*self).deref().get(backend)
    }

    #[inline]
    fn append(&self, backend: &mut dyn StateBackend, value: IN) -> ArconResult<()> {
        (*self).deref().append(backend, value)
    }
}

impl<IK, N, IN, OUT> MergingState<dyn StateBackend, IK, N, IN, OUT>
    for BoxedAggregatingState<IN, OUT, IK, N>
{
}

impl<IK, N, IN, OUT> AggregatingState<dyn StateBackend, IK, N, IN, OUT>
    for BoxedAggregatingState<IN, OUT, IK, N>
{
}
