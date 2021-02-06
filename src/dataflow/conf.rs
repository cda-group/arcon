// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconType, StateID},
    stream::{operator::Operator, time::ArconTime},
};
use arcon_error::*;
use arcon_state::index::{ArconState, EMPTY_STATE_ID};
use hocon::HoconLoader;
use serde::Deserialize;
use std::{path::Path, sync::Arc};

// Defines a Default State Backend for high-level operators that do not use any
// custom-defined state but still need a backend defined for internal runtime state.
cfg_if::cfg_if! {
    if #[cfg(feature = "rocksdb")]  {
        pub type DefaultBackend = arcon_state::Rocks;
    } else {
        pub type DefaultBackend = arcon_state::Sled;
    }
}

/// Defines how the runtime will manage the
/// parallelism for a specific Arcon Operator.
#[derive(Deserialize, Clone, Debug)]
pub enum ParallelismStrategy {
    /// Use a static number of Arcon nodes
    Static(usize),
    /// Tells the runtime to manage the parallelism
    Managed,
}

impl Default for ParallelismStrategy {
    fn default() -> Self {
        // static for now until managed is complete and stable..
        //ParallelismStrategy::Static(num_cpus::get() / 2)
        ParallelismStrategy::Static(1)
    }
}

#[derive(Deserialize, Clone, Debug)]
pub struct OperatorConf {
    /// Parallelism Strategy for this Operator
    pub parallelism_strategy: ParallelismStrategy,
    /// The highest possible key value for a keyed stream
    ///
    /// This should not be set too low or ridiculously high
    pub max_key: u64,
}
impl OperatorConf {
    pub fn from_file(path: impl AsRef<Path>) -> ArconResult<OperatorConf> {
        let data = std::fs::read_to_string(path)
            .map_err(|e| arcon_err_kind!("Failed to read config file with err {}", e))?;

        let loader: HoconLoader = HoconLoader::new()
            .load_str(&data)
            .map_err(|e| arcon_err_kind!("Failed to load Hocon Loader with err {}", e))?;

        let conf = loader
            .resolve()
            .map_err(|e| arcon_err_kind!("Failed to resolve ArconConf with err {}", e))?;

        Ok(conf)
    }
}

impl Default for OperatorConf {
    fn default() -> Self {
        Self {
            parallelism_strategy: Default::default(),
            max_key: 256,
        }
    }
}

#[derive(Clone)]
pub struct OperatorBuilder<OP: Operator, Backend = DefaultBackend> {
    /// Operator Constructor
    pub constructor: Arc<dyn Fn(Arc<Backend>) -> OP + Send + Sync + 'static>,
    /// Operator Config
    pub conf: OperatorConf,
}

impl<OP: Operator, Backend: arcon_state::Backend> OperatorBuilder<OP, Backend> {
    pub(crate) fn create_backend(&self, state_dir: std::path::PathBuf) -> Arc<Backend> {
        Arc::new(Backend::create(&state_dir).unwrap())
    }

    pub(crate) fn state_id(&self) -> StateID {
        let mut state_id = OP::OperatorState::STATE_ID.to_owned();
        if state_id == EMPTY_STATE_ID {
            // create unique identifier so there is no clash between empty states
            let unique_id = uuid::Uuid::new_v4().to_string();
            state_id = format!("{}_{}", state_id, unique_id);
        }
        state_id
    }
}

pub type TimestampExtractor<A> = Arc<dyn Fn(&A) -> u64 + Send + Sync>;

#[derive(Clone)]
pub struct SourceConf<S: ArconType> {
    pub extractor: Option<TimestampExtractor<S>>,
    pub time: ArconTime,
}

impl<S: ArconType> SourceConf<S> {
    pub fn set_arcon_time(&mut self, time: ArconTime) {
        self.time = time;
    }
    pub fn set_timestamp_extractor(&mut self, f: impl Fn(&S) -> u64 + Send + Sync + 'static) {
        self.extractor = Some(Arc::new(f));
    }
}

impl<S: ArconType> Default for SourceConf<S> {
    fn default() -> Self {
        Self {
            extractor: None,
            time: Default::default(),
        }
    }
}
