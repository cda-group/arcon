// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

pub mod handles;
#[macro_use]
pub mod macros;
pub mod ops;
pub mod serialization;

#[cfg(test)]
#[macro_use]
pub mod test_common;

pub use crate::{
    handles::Handle,
    ops::{AggregatorOps, MapOps, ReducerOps, ValueOps, VecOps},
};

use crate::{
    data::{Key, Metakey, Value},
    error::*,
};
use std::{
    any,
    collections::{BTreeSet, HashMap},
    fmt,
    fmt::{Debug, Formatter},
    fs,
    marker::PhantomData,
    path::{Path, PathBuf},
};

#[derive(Debug, Default)]
pub struct Config {
    pub live_state_base_path: PathBuf,
    pub checkpoints_base_path: PathBuf,
    pub backend_ids: Vec<String>,
}

pub trait Backend:
    ValueOps + MapOps + VecOps + ReducerOps + AggregatorOps + Send + Sync + 'static
{
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
    fn register_value_handle<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &mut Handle<ValueState<T>, IK, N>,
    );
    fn register_map_handle<K: Key, V: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &mut Handle<MapState<K, V>, IK, N>,
    );
    fn register_vec_handle<T: Value, IK: Metakey, N: Metakey>(
        &self,
        handle: &mut Handle<VecState<T>, IK, N>,
    );
    fn register_reducer_handle<T: Value, F: Reducer<T>, IK: Metakey, N: Metakey>(
        &self,
        handle: &mut Handle<ReducerState<T, F>, IK, N>,
    );
    fn register_aggregator_handle<A: Aggregator, IK: Metakey, N: Metakey>(
        &self,
        handle: &mut Handle<AggregatorState<A>, IK, N>,
    );
    // endregion
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

//pub mod in_memory;
//pub use self::in_memory::InMemory;
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
    //InMemory,
    //MeteredInMemory,
    #[cfg(feature = "rocks")]
    Rocks,
    //#[cfg(feature = "rocks")]
    //MeteredRocks,
    #[cfg(feature = "sled")]
    Sled,
    //#[cfg(feature = "sled")]
    //MeteredSled,
    #[cfg(all(feature = "faster", target_os = "linux"))]
    Faster,
    //#[cfg(all(feature = "faster", target_os = "linux"))]
    //MeteredFaster,
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
            //InMemory,
            //MeteredInMemory,
            #[cfg(feature = "rocks")]
            Rocks,
            //#[cfg(feature = "rocks")]
            //MeteredRocks,
            #[cfg(feature = "sled")]
            Sled,
            //#[cfg(feature = "sled")]
            //MeteredSled,
            #[cfg(all(feature = "faster", target_os = "linux"))]
            Faster,
            //#[cfg(all(feature = "faster", target_os = "linux"))]
            //MeteredFaster,
        ]
    };

    pub const STR_VARIANTS: &'static [&'static str] = {
        &[
            //"InMemory",
            //"MeteredInMemory",
            #[cfg(feature = "rocks")]
            "Rocks",
            //#[cfg(feature = "rocks")]
            //"MeteredRocks",
            #[cfg(feature = "sled")]
            "Sled",
            //#[cfg(feature = "sled")]
            //"MeteredSled",
            #[cfg(all(feature = "faster", target_os = "linux"))]
            "Faster",
            //#[cfg(all(feature = "faster", target_os = "linux"))]
            //"MeteredFaster",
        ]
    };
}

use std::str::FromStr;
impl FromStr for BackendType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use BackendType::*;
        match s {
            //x if x.eq_ignore_ascii_case("InMemory") => Ok(InMemory),
            //x if x.eq_ignore_ascii_case("MeteredInMemory") => Ok(MeteredInMemory),
            #[cfg(feature = "rocks")]
            x if x.eq_ignore_ascii_case("Rocks") => Ok(Rocks),
            //#[cfg(feature = "rocks")]
            //x if x.eq_ignore_ascii_case("MeteredRocks") => Ok(MeteredRocks),
            #[cfg(feature = "sled")]
            x if x.eq_ignore_ascii_case("Sled") => Ok(Sled),
            //#[cfg(feature = "sled")]
            //x if x.eq_ignore_ascii_case("MeteredSled") => Ok(MeteredSled),
            #[cfg(all(feature = "faster", target_os = "linux"))]
            x if x.eq_ignore_ascii_case("Faster") => Ok(Faster),
            //#[cfg(all(feature = "faster", target_os = "linux"))]
            //x if x.eq_ignore_ascii_case("MeteredFaster") => Ok(MeteredFaster),
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
