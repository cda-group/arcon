// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
#![feature(associated_type_defaults)]
#![feature(const_generics)]
#![warn(missing_debug_implementations)]
//use crate::{error::*, metered::Metrics, serialization::fixed_bytes::FixedBytes};
use crate::{error::*, serialization::fixed_bytes::FixedBytes};
use std::{
    any,
    cell::{RefCell, RefMut},
    collections::{BTreeSet, HashMap},
    fmt,
    fmt::{Debug, Formatter},
    fs,
    marker::PhantomData,
    path::{Path, PathBuf},
};

pub mod error;
pub mod handles;
#[macro_use]
pub mod macros;
mod hint;
pub mod index;
pub mod ops;
pub mod serialization;

#[cfg(test)]
#[macro_use]
pub mod test_common;

pub use crate::{
    handles::Handle,
    ops::{AggregatorOps, MapOps, ReducerOps, ValueOps, VecOps},
};

pub trait Value: prost::Message + Default + Clone + 'static {}
impl<T> Value for T where T: prost::Message + Default + Clone + 'static {}

pub trait Key: prost::Message + Default + Clone + 'static {}
impl<T> Key for T where T: prost::Message + Default + Clone + 'static {}

pub trait Metakey: FixedBytes + Copy + Clone + Send + Sync + 'static {}
impl<T> Metakey for T where T: FixedBytes + Copy + Clone + Send + Sync + 'static {}

#[derive(Debug, Default)]
pub struct Config {
    pub live_state_base_path: PathBuf,
    pub checkpoints_base_path: PathBuf,
    pub backend_ids: Vec<String>,
}

pub trait Backend:
    ValueOps + MapOps + VecOps + ReducerOps + AggregatorOps + Send + 'static
{
    fn restore_or_create(config: &Config, id: String) -> Result<BackendContainer<Self>>
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

    fn create(live_path: &Path) -> Result<BackendContainer<Self>>
    where
        Self: Sized;
    fn restore(live_path: &Path, checkpoint_path: &Path) -> Result<BackendContainer<Self>>
    where
        Self: Sized;

    fn was_restored(&self) -> bool;

    fn checkpoint(&self, checkpoint_path: &Path) -> Result<()>;

    /// should not be called from outside `BackendContainer::session`
    fn start_session(&mut self) {}

    /// should not be called from outside `BackendContainer::session`
    fn session_drop_hook(&mut self) -> Option<Box<dyn FnOnce(&mut Self)>> {
        None
    }

    /*
    fn metrics(&mut self) -> Option<&mut Metrics> {
        None
    }
    */

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

#[derive(Debug)]
pub struct BackendContainer<B: Backend> {
    pub inner: RefCell<B>,
}

impl<B: Backend> BackendContainer<B> {
    fn new(backend: B) -> Self {
        BackendContainer {
            inner: RefCell::new(backend),
        }
    }

    pub fn get_mut(&mut self) -> &mut B {
        self.inner.get_mut()
    }

    pub fn session(&self) -> Session<B> {
        let mut backend = self.inner.borrow_mut();
        backend.start_session();
        let drop_hook = backend.session_drop_hook();

        Session { backend, drop_hook }
    }
}

pub struct Session<'b, B: ?Sized> {
    /// DANGER: _never_ overwrite this
    pub backend: RefMut<'b, B>,
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
            Some(drop_hook) => drop_hook(&mut *self.backend),
            None => (),
        }
    }
}

mod reg_token {
    use super::*;
    #[derive(Debug)]
    pub struct RegistrationToken<'b, B>(pub(crate) &'b mut B);
    impl<'s, B: Backend> RegistrationToken<'s, B> {
        /// This is only safe to call by an Arcon Node. The registration token has to be used
        /// before any state backend operations happen
        pub unsafe fn new<'b>(session: &'s mut Session<'b, B>) -> Self {
            RegistrationToken(&mut *session.backend)
        }
    }
}
pub use self::reg_token::RegistrationToken;

// In an ideal world where rust has working GATs, there would be 0 generics here...
pub trait Bundle<'this, 'session, 'backend, B: Backend>: Send {
    // ... because they would be here instead. And everybody would be happy.
    type Active;
    fn register_states(&mut self, registration_token: &mut RegistrationToken<B>);
    fn activate(&'this self, session: &'session mut Session<'backend, B>) -> Self::Active;
}

impl<'this, 'session, 'backend, B: Backend> Bundle<'this, 'session, 'backend, B> for () {
    type Active = ();
    fn register_states(&mut self, _registration_token: &mut RegistrationToken<B>) {}
    fn activate(&self, _session: &mut Session<B>) -> () {
        ()
    }
}

// trait alias analogous to DeserializeOwned in serde
// would be cleaner if we had GATs
pub trait GenericBundle<B: Backend>:
    for<'this, 'session, 'backend> Bundle<'this, 'session, 'backend, B>
{
}

impl<T, B: Backend> GenericBundle<B> for T where
    T: for<'this, 'session, 'backend> Bundle<'this, 'session, 'backend, B>
{
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

pub mod in_memory;
pub use self::in_memory::InMemory;
//pub mod metered;
//pub use self::metered::Metered;

#[cfg(feature = "rocks")]
pub mod rocks;
#[cfg(feature = "rocks")]
pub use self::rocks::Rocks;

#[cfg(all(feature = "faster", target_os = "linux"))]
pub mod faster;
#[cfg(all(feature = "faster", target_os = "linux"))]
pub use self::faster::Faster;

#[cfg(feature = "sled")]
pub mod sled;
#[cfg(feature = "sled")]
pub use self::sled::Sled;

#[derive(PartialEq, Eq, Copy, Clone, Debug)]
pub enum BackendType {
    InMemory,
    MeteredInMemory,
    #[cfg(feature = "rocks")]
    Rocks,
    #[cfg(feature = "rocks")]
    MeteredRocks,
    #[cfg(feature = "sled")]
    Sled,
    #[cfg(feature = "sled")]
    MeteredSled,
    #[cfg(all(feature = "faster", target_os = "linux"))]
    Faster,
    #[cfg(all(feature = "faster", target_os = "linux"))]
    MeteredFaster,
}

impl fmt::Display for BackendType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl BackendType {
    pub const VARIANTS: &'static [BackendType] = {
        use BackendType::*;
        &[
            InMemory,
            MeteredInMemory,
            #[cfg(feature = "rocks")]
            Rocks,
            #[cfg(feature = "rocks")]
            MeteredRocks,
            #[cfg(feature = "sled")]
            Sled,
            #[cfg(feature = "sled")]
            MeteredSled,
            #[cfg(all(feature = "faster", target_os = "linux"))]
            Faster,
            #[cfg(all(feature = "faster", target_os = "linux"))]
            MeteredFaster,
        ]
    };

    pub const STR_VARIANTS: &'static [&'static str] = {
        &[
            "InMemory",
            "MeteredInMemory",
            #[cfg(feature = "rocks")]
            "Rocks",
            #[cfg(feature = "rocks")]
            "MeteredRocks",
            #[cfg(feature = "sled")]
            "Sled",
            #[cfg(feature = "sled")]
            "MeteredSled",
            #[cfg(all(feature = "faster", target_os = "linux"))]
            "Faster",
            #[cfg(all(feature = "faster", target_os = "linux"))]
            "MeteredFaster",
        ]
    };
}

use std::str::FromStr;
impl FromStr for BackendType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use BackendType::*;
        match s {
            x if x.eq_ignore_ascii_case("InMemory") => Ok(InMemory),
            x if x.eq_ignore_ascii_case("MeteredInMemory") => Ok(MeteredInMemory),
            #[cfg(feature = "rocks")]
            x if x.eq_ignore_ascii_case("Rocks") => Ok(Rocks),
            #[cfg(feature = "rocks")]
            x if x.eq_ignore_ascii_case("MeteredRocks") => Ok(MeteredRocks),
            #[cfg(feature = "sled")]
            x if x.eq_ignore_ascii_case("Sled") => Ok(Sled),
            #[cfg(feature = "sled")]
            x if x.eq_ignore_ascii_case("MeteredSled") => Ok(MeteredSled),
            #[cfg(all(feature = "faster", target_os = "linux"))]
            x if x.eq_ignore_ascii_case("Faster") => Ok(Faster),
            #[cfg(all(feature = "faster", target_os = "linux"))]
            x if x.eq_ignore_ascii_case("MeteredFaster") => Ok(MeteredFaster),
            _ => Err(format!(
                "valid values: {}",
                BackendType::VARIANTS
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
        }
    }
}
