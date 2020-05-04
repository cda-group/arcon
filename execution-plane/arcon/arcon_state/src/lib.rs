// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
#![feature(associated_type_defaults)]
#![feature(const_generics)]
#![warn(missing_debug_implementations)]
use crate::{error::*, handles::ActiveHandle, serialization::fixed_bytes::FixedBytes};
use std::{
    any, fmt,
    fmt::{Debug, Formatter},
    marker::PhantomData,
    path::PathBuf,
};

pub mod error;
pub mod handles;
pub mod serialization;
use crate::handles::BoxedIteratorOfResult;
pub use handles::Handle;

pub trait Value: prost::Message + Default + Clone + 'static {}
impl<T> Value for T where T: prost::Message + Default + Clone + 'static {}

pub trait Key: prost::Message + Default + 'static {}
impl<T> Key for T where T: prost::Message + Default + 'static {}

pub trait Metakey: FixedBytes + Send + Sync + 'static {}
impl<T> Metakey for T where T: FixedBytes + Send + Sync + 'static {}

#[derive(Debug, Default)]
pub struct Config {
    live_state_base_path: PathBuf,
    checkpoints_base_path: PathBuf,
    backend_ids: Vec<String>,
}

pub trait Backend: Send {
    fn restore_or_create(config: &Config, id: String) -> Result<Self>
    where
        Self: Sized;
    fn checkpoint(&self, config: &Config) -> Result<()>;
    fn session(&mut self) -> Session<Self> {
        Session {
            backend: self,
            drop_hook: None,
        }
    }

    // region active handle builders
    fn build_active_value_handle<'s, T: Value, IK: Metakey, N: Metakey>(
        &'s mut self,
        inner: &'s mut Handle<ValueState<T>, IK, N>,
    ) -> ActiveHandle<'s, Self, ValueState<T>, IK, N>;
    fn build_active_map_handle<'s, K: Key, V: Value, IK: Metakey, N: Metakey>(
        &'s mut self,
        inner: &'s mut Handle<MapState<K, V>, IK, N>,
    ) -> ActiveHandle<'s, Self, MapState<K, V>, IK, N>;
    fn build_active_vec_handle<'s, T: Value, IK: Metakey, N: Metakey>(
        &'s mut self,
        inner: &'s mut Handle<VecState<T>, IK, N>,
    ) -> ActiveHandle<'s, Self, VecState<T>, IK, N>;
    fn build_active_reducer_handle<'s, T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &'s mut self,
        inner: &'s mut Handle<ReducerState<T, F>, IK, N>,
    ) -> ActiveHandle<'s, Self, ReducerState<T, F>, IK, N>;
    fn build_active_aggregator_handle<'s, A: Aggregator, IK: Metakey, N: Metakey>(
        &'s mut self,
        inner: &'s mut Handle<AggregatorState<A>, IK, N>,
    ) -> ActiveHandle<'s, Self, AggregatorState<A>, IK, N>;
    // endregion

    // region value ops
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
    // endregion

    // region map ops
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
    // endregion

    // region vec ops
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
        value: impl IntoIterator<Item = T>,
    ) -> Result<()>;
    fn vec_len<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<usize>;
    fn vec_is_empty<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &Handle<VecState<T>, IK, N>,
    ) -> Result<bool>;
    // endregion

    // region reducer ops
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
    // endregion

    // region aggregator ops
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
    // endregion
}

pub struct Session<'b, B: ?Sized> {
    #[doc(hidden)]
    pub backend: &'b mut B,
    drop_hook: Option<Box<dyn FnOnce(&mut B)>>,
}

impl<B> Debug for Session<'_, B> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Session<{}>", any::type_name::<B>())
    }
}

impl<'b, B: ?Sized> Drop for Session<'b, B> {
    fn drop(&mut self) {
        match self.drop_hook.take() {
            Some(drop_hook) => drop_hook(self.backend),
            None => (),
        }
    }
}

pub trait Bundle<'this, 'b, B: Backend> {
    type Active;
    fn register_states<'s>(&mut self, session: &'b mut Session<'s, B>);
    fn activate<'s>(&'this mut self, session: &'b mut Session<'s, B>) -> Self::Active;
}

pub trait StateType: Default {
    type ExtraData = ();
}

#[derive(Debug)]
pub struct ValueState<T: Value>(PhantomData<T>);
impl<T: Value> StateType for ValueState<T> {}
impl<T: Value> Default for ValueState<T> {
    fn default() -> Self {
        ValueState(Default::default())
    }
}

#[derive(Debug)]
pub struct MapState<K: Key, V: Value>(PhantomData<(K, V)>);
impl<K: Key, V: Value> StateType for MapState<K, V> {}
impl<K: Key, V: Value> Default for MapState<K, V> {
    fn default() -> Self {
        MapState(Default::default())
    }
}

#[derive(Debug)]
pub struct VecState<T: Value>(PhantomData<T>);
impl<T: Value> StateType for VecState<T> {}
impl<T: Value> Default for VecState<T> {
    fn default() -> Self {
        VecState(Default::default())
    }
}

pub trait Reducer<T>: Fn(&T, &T) -> T {}
impl<F, T> Reducer<T> for F where F: Fn(&T, &T) -> T {}

#[derive(Debug)]
pub struct ReducerState<T: Value, F: Reducer<T>>(PhantomData<(T, F)>);
impl<T: Value, F: Reducer<T>> StateType for ReducerState<T, F> {
    type ExtraData = F;
}
impl<T: Value, F: Reducer<T>> Default for ReducerState<T, F> {
    fn default() -> Self {
        ReducerState(Default::default())
    }
}

pub trait Aggregator {
    type Input;
    type Accumulator: Value;
    type Result;

    fn create_accumulator(&self) -> Self::Accumulator;
    fn add(&self, acc: &mut Self::Accumulator, value: Self::Input);
    fn merge_accumulators(
        &self,
        fst: Self::Accumulator,
        snd: Self::Accumulator,
    ) -> Self::Accumulator;
    fn accumulator_into_result(&self, acc: Self::Accumulator) -> Self::Result;
}

#[derive(Debug)]
pub struct AggregatorState<A: Aggregator>(PhantomData<A>);
impl<A: Aggregator> StateType for AggregatorState<A> {
    type ExtraData = A;
}
impl<A: Aggregator> Default for AggregatorState<A> {
    fn default() -> Self {
        AggregatorState(Default::default())
    }
}

/// Macro to implement [`Bundle`]s.
///
/// [`Bundle`]: trait.Bundle.html
///
/// # Examples
///
/// ```
/// # #[macro_use] extern crate arcon_state;
/// # use arcon_state::*;
/// # use arcon_state::dummy::*;
/// # let mut backend = DummyBackend;
/// # let mut backend_session = backend.session();
/// bundle! {
///     /// My test bundle
///     pub struct MyTestBundle<T: Value> {
///         /// some value
///         value: Handle<ValueState<T>>,
///         /// some map
///         map: Handle<MapState<String, f64>, i32, u32>
///     }
/// }
///
/// impl<T: Value> MyTestBundle<T> {
///     fn new() -> Self {
///         MyTestBundle {
///             value: Handle::value("value"),
///             map: Handle::map("map").with_item_key(-1).with_namespace(0)
///         }       
///     }   
/// }
///
/// let mut bundle = MyTestBundle::<u32>::new();
/// // Bundle should be registered before usage, but we'll skip this in this example.
/// // Registration is normally done by an arcon Node early on, so you shouldn't create
/// // new instances of your bundles after that.
/// let mut active_bundle = bundle.activate(&mut backend_session);
/// let value_handle = active_bundle.value();
/// let map_handle = active_bundle.map();
/// ```
/// Note that if you want to have more bounds on your generic parameter, you'll have to use slightly
/// different syntax than what you're used to from regular Rust:
/// ```ignore
/// pub struct ParamWithManyBounds<T: Default: Clone> { // instead of T: Default + Clone
/// ```
/// Also, where clauses are unsupported.
// this might be nicer as a derive macro, but IntelliJ Rust can see through macro_rules!, so this
// version is nicer for people that use that
#[macro_export]
macro_rules! bundle {
    (
        $(#[$bundle_meta:meta])*
        $vis:vis struct $name:ident $(<
            $($generic_lifetime_param:lifetime),*$(,)?
            $($generic_param:ident $(: $first_bound:path $(: $other_bounds:path)*)?),*$(,)?
        >)? {$(
            $(#[$state_meta:meta])*
            $state_name:ident : Handle<$state_type:ty $(, $item_key_type:ty $(, $namespace_type:ty)?)?>
        ),*$(,)?}
    ) => {
        $(#[$bundle_meta])*
        $vis struct $name$(<
            $($generic_lifetime_param,)*
            $($generic_param $(: $first_bound $(+ $other_bounds)*)?,)*
        >)? {
            $(
                $(#[$state_meta])*
                $state_name : $crate::Handle<$state_type $(, $item_key_type $(, $namespace_type)?)?>,
            )*
        }

        const _: () = {
            pub struct Active<
                '__bundle, '__backend, $($($generic_lifetime_param,)*)?
                __B: $crate::Backend,
                $($($generic_param $(: $first_bound $(+ $other_bounds)*)?,)*)?
            > {
                backend: &'__backend mut __B,
                inner: &'__bundle mut $name$(<
                    $($generic_lifetime_param,)*
                    $($generic_param,)*
                >)?,
            }

            impl<
                '__bundle, '__backend, $($($generic_lifetime_param,)*)?
                __B: $crate::Backend,
                $($($generic_param $(: $first_bound $(+ $other_bounds)*)?,)*)?
            > Active<
                '__bundle, '__backend, $($($generic_lifetime_param,)*)?
                __B, $($($generic_param,)*)?
            > {$(
                $(#[$state_meta])*
                fn $state_name(&mut self) -> $crate::handles::ActiveHandle<__B,
                    $state_type $(, $item_key_type $(, $namespace_type)?)?
                > {
                    self.inner.$state_name.activate(&mut self.backend)
                }
            )*}

            impl<
                '__this, '__backend, $($($generic_lifetime_param,)*)?
                __B: $crate::Backend + '__backend,
                $($($generic_param : '__this $(+ $first_bound $(+ $other_bounds)*)?,)*)?
            > $crate::Bundle<'__this, '__backend, __B> for $name$(<
                $($generic_lifetime_param,)* $($generic_param,)*
            >)? {
                type Active = Active<
                    '__this, '__backend, $($($generic_lifetime_param,)*)?
                    __B, $($($generic_param,)*)?
                >;

                fn register_states<'s>(&mut self, session: &'__backend mut $crate::Session<'s, __B>) {
                    let mut active = self.activate(session);
                    $(active.$state_name();)*
                }

                fn activate<'s>(
                    &'__this mut self,
                    session: &'__backend mut $crate::Session<'s, __B>,
                ) -> Self::Active {
                    Active {
                        backend: session.backend,
                        inner: self,
                    }
                }
            }
        };
    };
}

#[doc(hidden)]
pub mod dummy;
pub mod in_memory;
pub use in_memory::InMemory;

#[cfg(test)]
mod tests {
    use super::*;

    bundle! {
        struct MyBundle<F: Fn(&i32, &i32) -> i32> {
            value: Handle<ValueState<u32>>,
            map: Handle<MapState<String, f64>, i8, u32>,
            reducer: Handle<ReducerState<i32, F>>,
        }
    }

    impl<F: Fn(&i32, &i32) -> i32> MyBundle<F> {
        fn new(reducer: F) -> MyBundle<F> {
            MyBundle {
                value: Handle::value("mybundle.value"),
                map: Handle::map("mybundle.map")
                    .with_item_key(-1)
                    .with_namespace(0),
                reducer: Handle::reducer("mybundle.reducer", reducer),
            }
        }
    }

    #[test]
    fn dummy_test() {
        let mut backend = dummy::DummyBackend;
        let mut session = backend.session();
        let mut bundle = MyBundle::new(|a: &i32, b: &i32| a + b);
        {
            let mut bundle = bundle.activate(&mut session);
            let value = bundle.value();
            dbg!(value);
            let map = bundle.map();
            dbg!(map);
            let reducer = bundle.reducer();
            dbg!(reducer);
        }
        {
            let mut bundle = bundle.activate(&mut session);
            let value = bundle.value();
            dbg!(value);
            let map = bundle.map();
            dbg!(map);
            let reducer = bundle.reducer();
            dbg!(reducer);
        }
    }
}
