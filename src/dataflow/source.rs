// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use super::stream::DefaultBackend;
use crate::{
    data::{ArconType, StateID},
    stream::time::ArconTime,
};
use arcon_state::index::ArconState;
use std::{marker::PhantomData, sync::Arc};

pub struct OperatorConf<A, State, Backend = DefaultBackend>
where
    A: ArconType,
    State: ArconState,
    Backend: arcon_state::Backend,
{
    _marker: PhantomData<A>,
    state_cons: Option<Arc<dyn Fn(Arc<Backend>) -> State + Send + Sync + 'static>>,
}

impl<A, State, Backend> OperatorConf<A, State, Backend>
where
    A: ArconType,
    State: ArconState,
    Backend: arcon_state::Backend,
{
    pub fn new() -> Self {
        Self {
            state_cons: None,
            _marker: PhantomData,
        }
    }
    pub fn set_state_constructor(
        &mut self,
        f: impl Fn(Arc<Backend>) -> State + Send + Sync + 'static,
    ) {
        self.state_cons = Some(Arc::new(f));
    }
    pub fn set_state_id(&mut self, state_id: impl Into<StateID>) {}
}

impl<A> Default for OperatorConf<A, (), DefaultBackend>
where
    A: ArconType,
{
    fn default() -> Self {
        Self {
            state_cons: Some(Arc::new(|_| ())),
            _marker: PhantomData,
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
