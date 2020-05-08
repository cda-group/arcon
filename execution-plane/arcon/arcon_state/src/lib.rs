// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
#![feature(associated_type_defaults)]
#![feature(const_generics)]
#![warn(missing_debug_implementations)]
use crate::{error::*, serialization::fixed_bytes::FixedBytes};
use std::{
    any,
    collections::{BTreeSet, HashMap},
    fmt,
    fmt::{Debug, Formatter},
    fs,
    marker::PhantomData,
    path::{Path, PathBuf},
};

pub mod error;
pub mod handles;
pub mod ops;
pub mod serialization;
pub use crate::{
    handles::Handle,
    ops::{AggregatorOps, MapOps, ReducerOps, ValueOps, VecOps},
};

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

pub trait Backend: ValueOps + MapOps + VecOps + ReducerOps + AggregatorOps + Send {
    fn restore_or_create(config: &Config, id: String) -> Result<Self>
    where
        Self: Sized,
    {
        // TODO: make IO errors have more context
        let mut state_path = config.live_state_base_path.clone();
        state_path.push(&id);
        if state_path.exists() {
            fs::remove_dir_all(&state_path)?;
        }

        let mut checkpoints: HashMap<&str, BTreeSet<u64>> = config
            .backend_ids
            .iter()
            .map(|id| (id.as_str(), BTreeSet::new()))
            .collect();

        for directory in fs::read_dir(&config.checkpoints_base_path)? {
            let directory = directory?;

            let invalid_path = || InvalidPath {
                path: directory.path(),
            };

            let dir_name = directory.file_name();
            let dir_name = dir_name.to_str().with_context(invalid_path)?;

            const CHECKPOINT_PREFIX: &str = "checkpoint_";

            ensure!(
                dir_name.starts_with(CHECKPOINT_PREFIX)
                    && directory.metadata().map(|m| m.is_dir()).unwrap_or(false),
                InvalidPath {
                    path: directory.path()
                }
            );

            let dir_name = &dir_name[CHECKPOINT_PREFIX.len()..];
            let mut dir_name_parts = dir_name.split('_');
            let id = dir_name_parts.next().with_context(invalid_path)?;
            let epoch: u64 = dir_name_parts
                .next()
                .with_context(invalid_path)?
                .parse()
                .ok()
                .with_context(invalid_path)?;

            ensure!(dir_name_parts.next().is_none(), InvalidPath {
                path: directory.path(),
            });

            let checkpoints_for_id = checkpoints.get_mut(id).with_context(|| UnknownNode {
                unknown_node: id.to_string(),
                known_nodes: config.backend_ids.clone(),
            })?;

            checkpoints_for_id.insert(epoch);
        }

        let mut checkpoints = checkpoints.into_iter();
        let mut complete_checkpoints = checkpoints.next().map(|x| x.1);
        // complete checkpoints are the ones that are in in every checkpoint set,
        // so we just intersect all the sets
        if let Some(complete) = &mut complete_checkpoints {
            for (_, other) in checkpoints {
                *complete = complete.intersection(&other).copied().collect();
            }
        }

        let last_complete_checkpoint =
            complete_checkpoints.and_then(|ce| ce.iter().last().copied());

        match last_complete_checkpoint {
            Some(epoch) => {
                let mut latest_checkpoint_path = config.checkpoints_base_path.clone();
                latest_checkpoint_path.push(format!(
                    "checkpoint_{id}_{epoch}",
                    id = id,
                    epoch = epoch
                ));

                Self::restore(&state_path, &latest_checkpoint_path)
            }
            None => Self::create(&state_path),
        }
    }

    fn create(live_path: &Path) -> Result<Self>
    where
        Self: Sized;
    fn restore(live_path: &Path, checkpoint_path: &Path) -> Result<Self>
    where
        Self: Sized;

    fn checkpoint(&self, checkpoint_path: &Path) -> Result<()>;
    fn session(&mut self) -> Session<Self> {
        Session {
            backend: self,
            drop_hook: None,
        }
    }

    // region handle registration
    fn register_value_handle<'s, T: Value, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<ValueState<T>, IK, N>,
    );
    fn register_map_handle<'s, K: Key, V: Value, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<MapState<K, V>, IK, N>,
    );
    fn register_vec_handle<'s, T: Value, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<VecState<T>, IK, N>,
    );
    fn register_reducer_handle<'s, T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<ReducerState<T, F>, IK, N>,
    );
    fn register_aggregator_handle<'s, A: Aggregator, IK: Metakey, N: Metakey>(
        &'s mut self,
        handle: &'s mut Handle<AggregatorState<A>, IK, N>,
    );
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

mod reg_token {
    #[derive(Debug)]
    pub struct RegistrationToken {
        _private_constructor: (),
    }
    impl RegistrationToken {
        /// This is only safe to call by an Arcon Node. The registration token has to be used
        /// before any state backend operations happen
        pub unsafe fn new() -> RegistrationToken {
            RegistrationToken {
                _private_constructor: (),
            }
        }
    }
}
pub use reg_token::RegistrationToken;

pub trait Bundle<'this, 'b, B: Backend> {
    type Active;
    fn register_states<'s>(
        &mut self,
        session: &'b mut Session<'s, B>,
        registration_token: &RegistrationToken,
    );
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

pub trait Reducer<T>: Fn(&T, &T) -> T + Send + Sync + Clone + 'static {}
impl<F, T> Reducer<T> for F where F: Fn(&T, &T) -> T + Send + Sync + Clone + 'static {}

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

pub trait Aggregator: Send + Sync + Clone + 'static {
    type Input: Value;
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
/// # use arcon_state::in_memory::*;
/// # let mut backend = InMemory::restore_or_create(&Default::default(), Default::default()).unwrap();
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
/// // Usually a bundle should be registered by an arcon node, but we'll do it manually here
/// bundle.register_states(&mut backend_session, unsafe { &RegistrationToken::new() });
///
/// let mut active_bundle = bundle.activate(&mut backend_session);
/// let mut value_handle = active_bundle.value();
/// value_handle.set(3).unwrap();
/// println!("value is {:?}", value_handle.get().unwrap());
/// let mut map_handle = active_bundle.map();
/// map_handle.insert("foo".into(), 0.5).unwrap();
/// println!("map[foo] is {:?}", map_handle.get(&"foo".into()).unwrap())
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
                #[inline]
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

                fn register_states<'s>(
                    &mut self,
                    session: &'__backend mut $crate::Session<'s, __B>,
                    registration_token: &$crate::RegistrationToken
                ) {
                    $(self.$state_name.register(session, registration_token);)*
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

pub mod in_memory;
pub use in_memory::InMemory;
#[cfg(feature = "rocks")]
pub mod rocks;
#[cfg(feature = "rocks")]
pub use rocks::Rocks;
