// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconType, StateID},
    index::{ArconState, EMPTY_STATE_ID},
    stream::{operator::Operator, source::Source, time::ArconTime},
};
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
        ParallelismStrategy::Static(num_cpus::get() / 2)
    }
}

/// Defines whether a stream is Keyed or Local
///
/// Streams are by default Keyed in Arcon.
#[derive(Deserialize, Clone, Debug)]
pub enum StreamKind {
    Keyed,
    Local,
}

impl Default for StreamKind {
    fn default() -> Self {
        StreamKind::Keyed
    }
}

/// Operator Configuration
///
/// Defines how an Operator is to be executed on Arcon.
#[derive(Deserialize, Default, Clone, Debug)]
pub struct OperatorConf {
    /// Parallelism Strategy for this Operator
    pub parallelism_strategy: ParallelismStrategy,
    /// Defines the type of Stream, by default streams are Keyed in Arcon.
    pub stream_kind: StreamKind,
}

impl OperatorConf {
    /// Load an OperatorConf from a File using the Hocon format
    pub fn from_file(path: impl AsRef<Path>) -> OperatorConf {
        // okay to panic here as this is during setup code...
        let data = std::fs::read_to_string(path).unwrap();
        HoconLoader::new()
            .load_str(&data)
            .unwrap()
            .resolve()
            .unwrap()
    }
}

/// Operator Builder
///
/// Defines everything needed in order for Arcon to instantiate
/// and manage an Operator during runtime.
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

/// Source Configuration
#[derive(Clone)]
pub struct SourceConf<S: ArconType> {
    pub extractor: Option<TimestampExtractor<S>>,
    pub time: ArconTime,
}

impl<S: ArconType> SourceConf<S> {
    /// Set [ArconTime] to be used for a Source
    pub fn set_arcon_time(&mut self, time: ArconTime) {
        self.time = time;
    }
    /// Set a Timestamp Extractor for a Source
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

/// Source Builder
///
/// Defines how Sources are constructed and managed during runtime.
#[derive(Clone)]
pub struct SourceBuilder<S: Source, Backend = DefaultBackend> {
    /// Source Constructor
    pub constructor: Arc<dyn Fn(Arc<Backend>) -> S + Send + Sync + 'static>,
    /// Source Config
    pub conf: SourceConf<S::Data>,
}
