// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

extern crate static_assertions as sa;
#[cfg(test)]
extern crate tempfile;

use crate::state_backend::serialization::{
    DeserializableWith, SerializableFixedSizeWith, SerializableWith,
};
use arcon_error::ArconResult;
use state_types::*;
use std::any::{Any, TypeId};

// NOTE: we are using bincode for serialization, so it's probably not portable between architectures
// of different endianness

// TODO: a lot of params here could be borrows
// TODO: figure out if this needs to be Send + Sync
/// Trait required for all state backend implementations in Arcon
pub trait StateBackend: Any {
    fn new(path: &str) -> ArconResult<Self>
    where
        Self: Sized;

    fn checkpoint(&self, checkpoint_path: &str) -> ArconResult<()>;
    fn restore(restore_path: &str, checkpoint_path: &str) -> ArconResult<Self>
    where
        Self: Sized;
}

// This is copied from std::any, because rust trait inheritance kinda sucks. Even std::any has
// duplicated impls for different combinations of Any with marker traits such as Send and Sync :/
impl dyn StateBackend {
    pub fn is<SB: StateBackend>(&self) -> bool {
        let t = TypeId::of::<SB>();
        let concrete = self.type_id();
        t == concrete
    }

    fn downcast_ref<SB: StateBackend>(&self) -> ArconResult<&SB> {
        if self.is::<SB>() {
            unsafe { Ok(&*(self as *const dyn StateBackend as *const SB)) }
        } else {
            arcon_err!("dynamic backend reference is of wrong actual type")
        }
    }

    fn downcast_mut<SB: StateBackend>(&mut self) -> ArconResult<&mut SB> {
        if self.is::<SB>() {
            unsafe { Ok(&mut *(self as *mut dyn StateBackend as *mut SB)) }
        } else {
            arcon_err!("dynamic backend reference is of wrong actual type")
        }
    }
}

//// builders ////

macro_rules! impl_dynamic_builder {
    (
        $builder_name: ident <$($params: ident),* $(| $($builder_params: ident),*)?>
        where {$($bounds: tt)*};
        $state_name: ident $(extra: <$($state_param_name: path),*>)?;
        $builder_fn: ident $(extra: ($($arg_name: ident : $arg_ty: ty),*))?
    ) => {
        impl<$($params,)* $($($builder_params,)*)?> $builder_name<$($params,)* $($($builder_params,)*)?> for dyn StateBackend
        where $($bounds)*
        {
            type Type = Box<dyn $state_name<dyn StateBackend, $($params,)* $($($state_param_name,)*)?>>;

            fn $builder_fn(
                &mut self,
                name: &str,
                init_item_key: IK,
                init_namespace: N,
                $($($arg_name: $arg_ty,)*)?
                key_serializer: KS,
                value_serializer: TS,
            ) -> Self::Type {
                {
                    use crate::state_backend::in_memory::InMemory;
                    if let Ok(in_memory) = self.downcast_mut::<InMemory>() {
                        return $state_name::erase_backend_type(in_memory.$builder_fn(
                            name,
                            init_item_key,
                            init_namespace,
                            $($($arg_name,)*)?
                            key_serializer,
                            value_serializer,
                        ));
                    }
                }

                #[cfg(feature = "arcon_rocksdb")]
                {
                    use crate::state_backend::rocksdb::RocksDb;
                    if let Ok(rocks) = self.downcast_mut::<RocksDb>() {
                        return $state_name::erase_backend_type(rocks.$builder_fn(
                            name,
                            init_item_key,
                            init_namespace,
                            $($($arg_name,)*)?
                            key_serializer,
                            value_serializer,
                        ));
                    }
                }

                // NOTE: every implemented state backend should be added here

                unimplemented!("underlying type is not supported!")
            }
        }
    };
}

// ideally this would be a part of the StateBackend trait, but we lack generic associated types, and
// there are probably some other problems with that as well
pub trait ValueStateBuilder<IK, N, T, KS, TS> {
    type Type: ValueState<Self, IK, N, T>;
    fn new_value_state(
        &mut self,
        name: &str,
        init_item_key: IK,
        init_namespace: N,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type;
}

// NOTE: KS, TS are added in the proper place implicitly - makes the macro easier
// NOTE: but also more fragile
impl_dynamic_builder! {
    ValueStateBuilder<IK, N, T | KS, TS> where {
        IK: SerializableFixedSizeWith<KS> + 'static,
        N: SerializableFixedSizeWith<KS> + 'static,
        (): SerializableFixedSizeWith<KS>,
        T: SerializableWith<TS> + DeserializableWith<TS> + 'static,
        KS: 'static,
        TS: 'static,
    }; ValueState; new_value_state
}

pub trait MapStateBuilder<IK, N, K, V, KS, TS> {
    type Type: MapState<Self, IK, N, K, V>;
    fn new_map_state(
        &mut self,
        name: &str,
        init_item_key: IK,
        init_namespace: N,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type;
}

impl_dynamic_builder! {
    MapStateBuilder<IK, N, K, V | KS, TS> where {
        IK: SerializableFixedSizeWith<KS> + DeserializableWith<KS> + 'static,
        N: SerializableFixedSizeWith<KS> + DeserializableWith<KS> + 'static,
        (): SerializableFixedSizeWith<KS>,
        K: SerializableWith<KS> + DeserializableWith<KS> + 'static,
        V: SerializableWith<TS> + DeserializableWith<TS> + 'static,
        KS: Clone + 'static,
        TS: Clone + 'static,
    }; MapState; new_map_state
}

pub trait VecStateBuilder<IK, N, T, KS, TS> {
    type Type: VecState<Self, IK, N, T>;
    fn new_vec_state(
        &mut self,
        name: &str,
        init_item_key: IK,
        init_namespace: N,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type;
}

impl_dynamic_builder! {
    VecStateBuilder<IK, N, T | KS, TS> where {
        IK: SerializableFixedSizeWith<KS> + 'static,
        N: SerializableFixedSizeWith<KS> + 'static,
        (): SerializableWith<KS>,
        T: SerializableWith<TS> + DeserializableWith<TS> + 'static,
        KS: 'static,
        TS: 'static,
    }; VecState; new_vec_state
}

pub trait ReducingStateBuilder<IK, N, T, F, KS, TS> {
    type Type: ReducingState<Self, IK, N, T>;
    fn new_reducing_state(
        &mut self,
        name: &str,
        init_item_key: IK,
        init_namespace: N,
        reduce_fn: F,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type;
}

impl_dynamic_builder! {
    ReducingStateBuilder<IK, N, T | F, KS, TS> where {
        IK: SerializableFixedSizeWith<KS> + 'static,
        N: SerializableFixedSizeWith<KS> + 'static,
        (): SerializableWith<KS>,
        T: SerializableWith<TS> + DeserializableWith<TS> + 'static,
        KS: Send + Sync + Clone + 'static,
        TS: Send + Sync + Clone + 'static,
        F: Fn(&T, &T) -> T + Send + Sync + Clone + 'static
    }; ReducingState; new_reducing_state extra: (reduce_fn: F)
}

pub trait AggregatingStateBuilder<IK, N, T, AGG: Aggregator<T>, KS, TS> {
    type Type: AggregatingState<Self, IK, N, T, AGG::Result>;
    fn new_aggregating_state(
        &mut self,
        name: &str,
        init_item_key: IK,
        init_namespace: N,
        aggregator: AGG,
        key_serializer: KS,
        value_serializer: TS,
    ) -> Self::Type;
}

impl_dynamic_builder! {
    AggregatingStateBuilder<IK, N, T | AGG, KS, TS> where {
        AGG: Aggregator<T> + Send + Sync + Clone + 'static,
        IK: SerializableFixedSizeWith<KS> + 'static,
        N: SerializableFixedSizeWith<KS> + 'static,
        (): SerializableWith<KS>,
        AGG::Accumulator: SerializableWith<TS> + DeserializableWith<TS>,
        T: SerializableWith<TS> + DeserializableWith<TS> + 'static,
        KS: Send + Sync + Clone + 'static,
        TS: Send + Sync + Clone + 'static,
    }; AggregatingState extra: <AGG::Result>; new_aggregating_state extra: (aggregator: AGG)
}

#[macro_use]
mod state_types {
    // TODO: Q: Should methods that mutate the state actually take a mutable reference to self?
    // TODO: Q: For now this is modelled after Flink. Do we want a different hierarchy, or maybe get
    //  rid of the hierarchy altogether?

    use super::*;
    use std::{
        marker::PhantomData,
        ops::{Deref, DerefMut},
    };

    struct WithDynamicBackend<SB, C>(C, PhantomData<SB>);

    macro_rules! erase_backend_type {
        ($t: ident < _ $(, $rest: ident)* >) => {
            // `s` instead of self, because we don't want the method calling syntax for this,
            // because of ambiguities
            fn erase_backend_type(s: Self) -> Box<dyn $t <dyn StateBackend $(, $rest)*>>
            where
                Self: Sized + 'static,
                SB: StateBackend + Sized,
            {
                Box::new(WithDynamicBackend(s, Default::default()))
            }
        };
    }

    //// abstract states ////
    /// State inside a stream.
    ///
    /// `SB` - state backend type
    /// `IK` - type of key of the item currently in the stream
    /// `N`  - type of the namespace
    pub trait State<SB: ?Sized, IK, N> {
        fn clear(&self, backend: &mut SB) -> ArconResult<()>;

        fn get_current_key(&self) -> ArconResult<&IK>;
        fn set_current_key(&mut self, new_key: IK) -> ArconResult<()>;

        fn get_current_namespace(&self) -> ArconResult<&N>;
        fn set_current_namespace(&mut self, new_namespace: N) -> ArconResult<()>;

        erase_backend_type!(State<_, IK, N>);
    }

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

    macro_rules! delegate_key_and_namespace {
        ($common: ident) => {
            fn get_current_key(&self) -> ArconResult<&IK> {
                Ok(&self.$common.item_key)
            }

            fn set_current_key(&mut self, new_key: IK) -> ArconResult<()> {
                self.$common.item_key = new_key;
                Ok(())
            }

            fn get_current_namespace(&self) -> ArconResult<&N> {
                Ok(&self.$common.namespace)
            }

            fn set_current_namespace(&mut self, new_namespace: N) -> ArconResult<()> {
                self.$common.namespace = new_namespace;
                Ok(())
            }
        };
    }

    // TODO: since we don't have any state that is appending, but not merging, maybe consider using one trait?
    pub trait AppendingState<SB: ?Sized, IK, N, IN, OUT>: State<SB, IK, N> {
        fn get(&self, backend: &SB) -> ArconResult<OUT>;
        fn append(&self, backend: &mut SB, value: IN) -> ArconResult<()>;

        erase_backend_type!(AppendingState<_, IK, N, IN, OUT>);
    }

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

    pub trait MergingState<SB: ?Sized, IK, N, IN, OUT>: AppendingState<SB, IK, N, IN, OUT> {
        erase_backend_type!(MergingState<_, IK, N, IN, OUT>);
    }

    impl<SB, IK, N, IN, OUT, C> MergingState<dyn StateBackend, IK, N, IN, OUT>
        for WithDynamicBackend<SB, C>
    where
        C: MergingState<SB, IK, N, IN, OUT>,
        SB: StateBackend + Sized,
    {
    }

    impl_state_for_boxed_with_dyn_backend!(MergingState<_, IK, N, IN, OUT>);

    //// concrete-ish states ////

    pub trait ValueState<SB: ?Sized, IK, N, T>: State<SB, IK, N> {
        // bikeshed: get / value (Flink)
        fn get(&self, backend: &SB) -> ArconResult<T>;

        // bikeshed: set / update (Flink)
        fn set(&self, backend: &mut SB, new_value: T) -> ArconResult<()>;

        erase_backend_type!(ValueState<_, IK, N, T>);
    }

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

    pub trait MapState<SB: ?Sized, IK, N, K, V>: State<SB, IK, N> {
        fn get(&self, backend: &SB, key: &K) -> ArconResult<V>;
        fn put(&self, backend: &mut SB, key: K, value: V) -> ArconResult<()>;

        /// key_value_pairs must be a finite iterator!
        fn put_all_dyn(
            &self,
            backend: &mut SB,
            key_value_pairs: &mut dyn Iterator<Item = (K, V)>,
        ) -> ArconResult<()>;
        /// key_value_pairs must be a finite iterator!
        fn put_all(
            &self,
            backend: &mut SB,
            key_value_pairs: impl IntoIterator<Item = (K, V)>,
        ) -> ArconResult<()>
        where
            Self: Sized;

        fn remove(&self, backend: &mut SB, key: &K) -> ArconResult<()>;
        fn contains(&self, backend: &SB, key: &K) -> ArconResult<bool>;

        // unboxed iterators would require associated types generic over backend's lifetime
        // TODO: impl this when GATs land on nightly

        fn iter<'a>(&self, backend: &'a SB) -> ArconResult<Box<dyn Iterator<Item = (K, V)> + 'a>>;
        fn keys<'a>(&self, backend: &'a SB) -> ArconResult<Box<dyn Iterator<Item = K> + 'a>>;
        fn values<'a>(&self, backend: &'a SB) -> ArconResult<Box<dyn Iterator<Item = V> + 'a>>;

        fn is_empty(&self, backend: &SB) -> ArconResult<bool>;

        erase_backend_type!(MapState<_, IK, N, K, V>);
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

    // analogous to ListState in Flink
    // TODO: Q: Should MergingState::OUT be Vec, or something else? More abstract?
    pub trait VecState<SB: ?Sized, IK, N, T>: MergingState<SB, IK, N, T, Vec<T>> {
        // bikeshed: set / update (Flink)
        fn set(&self, backend: &mut SB, value: Vec<T>) -> ArconResult<()>;
        fn add_all(&self, backend: &mut SB, values: impl IntoIterator<Item = T>) -> ArconResult<()>
        where
            Self: Sized;
        fn add_all_dyn(
            &self,
            backend: &mut SB,
            values: &mut dyn Iterator<Item = T>,
        ) -> ArconResult<()>;

        //        fn len(&self, backend: &SB) -> ArconResult<usize>
        //        where
        //            T: SerializableFixedSizeWith<TS>; // can be problematic

        erase_backend_type!(VecState<_, IK, N, T>);
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

    pub trait ReducingState<SB: ?Sized, IK, N, T>: MergingState<SB, IK, N, T, T> {
        erase_backend_type!(ReducingState<_, IK, N, T>);
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

    pub trait AggregatingState<SB: ?Sized, IK, N, IN, OUT>:
        MergingState<SB, IK, N, IN, OUT>
    {
        erase_backend_type!(AggregatingState<_, IK, N, IN, OUT>);
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

    pub trait Aggregator<T> {
        type Accumulator;
        type Result;

        fn create_accumulator(&self) -> Self::Accumulator;
        // bikeshed - immutable + return value instead of mutable acc? (like in Flink)
        fn add(&self, acc: &mut Self::Accumulator, value: T);
        fn merge_accumulators(
            &self,
            fst: Self::Accumulator,
            snd: Self::Accumulator,
        ) -> Self::Accumulator;
        fn accumulator_into_result(&self, acc: Self::Accumulator) -> Self::Result;
    }

    #[derive(Clone)]
    pub struct ClosuresAggregator<CREATE, ADD, MERGE, RES> {
        create: CREATE,
        add: ADD,
        merge: MERGE,
        res: RES,
    }

    impl<CREATE, ADD, MERGE, RES> ClosuresAggregator<CREATE, ADD, MERGE, RES> {
        #[allow(dead_code)] // used by tests
        pub fn new<T, ACC, R>(
            create: CREATE,
            add: ADD,
            merge: MERGE,
            res: RES,
        ) -> ClosuresAggregator<CREATE, ADD, MERGE, RES>
        where
            CREATE: Fn() -> ACC,
            ADD: Fn(&mut ACC, T) -> (),
            RES: Fn(ACC) -> R,
            MERGE: Fn(ACC, ACC) -> ACC,
        {
            ClosuresAggregator {
                create,
                add,
                merge,
                res,
            }
        }
    }

    impl<CREATE, ADD, MERGE, RES, T> Aggregator<T> for ClosuresAggregator<CREATE, ADD, MERGE, RES>
    where
        CREATE: Fn<()>,
        ADD: Fn(&mut CREATE::Output, T) -> (),
        RES: Fn<(CREATE::Output,)>,
        MERGE: Fn(CREATE::Output, CREATE::Output) -> CREATE::Output,
    {
        type Accumulator = CREATE::Output;
        type Result = RES::Output;

        fn create_accumulator(&self) -> Self::Accumulator {
            (self.create)()
        }

        fn add(&self, acc: &mut Self::Accumulator, value: T) {
            (self.add)(acc, value)
        }

        fn merge_accumulators(
            &self,
            fst: Self::Accumulator,
            snd: Self::Accumulator,
        ) -> Self::Accumulator {
            (self.merge)(fst, snd)
        }

        fn accumulator_into_result(&self, acc: Self::Accumulator) -> Self::Result {
            (self.res)(acc)
        }
    }

    // TODO: broadcast state???

    sa::assert_obj_safe!(
        State<dyn StateBackend, u32, ()>,
        MapState<dyn StateBackend, i32, (), u32, u32>,
        ValueState<dyn StateBackend, i32, (), u32>,
        AppendingState<dyn StateBackend, i32, (), char, String>,
        MergingState<dyn StateBackend, i32, (), u32, std::collections::HashSet<u32>>,
        ReducingState<dyn StateBackend, i32, (), u32>,
        VecState<dyn StateBackend, i32, (), i32>,
        AggregatingState<dyn StateBackend, i32, (), i32, String>
    );
}

pub mod serialization;

pub mod in_memory;
#[cfg(feature = "arcon_rocksdb")]
pub mod rocksdb;
