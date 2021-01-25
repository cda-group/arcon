// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconType, StateID},
    stream::{operator::Operator, time::ArconTime},
};
use arcon_state::index::{ArconState, EMPTY_STATE_ID};
use std::sync::Arc;

// Defines a Default State Backend for high-level operators that do not use any
// custom-defined state but still need a backend defined for internal runtime state.
cfg_if::cfg_if! {
    if #[cfg(feature = "rocksdb")]  {
        pub type DefaultBackend = arcon_state::Rocks;
    } else {
        pub type DefaultBackend = arcon_state::Sled;
    }
}

#[derive(Clone)]
pub struct OperatorConf {
    /// Amount of instances to be created
    pub instances: usize,
    /// The highest possible key value for a keyed stream
    ///
    /// This should not be set too low or ridiculously high
    pub max_key: u64,
}

impl Default for OperatorConf {
    fn default() -> OperatorConf {
        Self {
            instances: 1,
            max_key: 256,
        }
    }
}

pub trait StreamKind {}
pub trait KeyedKind: StreamKind {}
pub trait LocalKind: StreamKind {}

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
