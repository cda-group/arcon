// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconType, StateID},
    stream::time::ArconTime,
};
use arcon_state::index::EMPTY_STATE_ID;
use std::sync::Arc;

// TODO: Add state/operator constructors?
// state_cons: Arc<dyn Fn(Arc<Backend>) -> OP::OperatorState + Send + Sync + 'static>,
// operator_cons: Arc<dyn Fn(Arc<Backend>) -> OP + Send + Sync + 'static>,
#[derive(Debug, Clone)]
pub struct OperatorConf {
    /// State ID for this DFG node
    pub(crate) state_id: StateID,
    /// Amount of instances to be created
    pub instances: usize,
}

impl OperatorConf {
    pub(crate) fn new(mut state_id: StateID) -> Self {
        if state_id == EMPTY_STATE_ID {
            // create unique identifier so there is no clash between empty states
            let unique_id = uuid::Uuid::new_v4().to_string();
            state_id = format!("{}_{}", state_id, unique_id);
        }
        Self {
            state_id,
            instances: 1,
        }
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
