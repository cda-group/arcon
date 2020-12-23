// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use super::stream::DefaultBackend;
use crate::{
    data::{ArconType, StateID},
    stream::{operator::Operator, time::ArconTime},
};
use std::sync::Arc;

// TODO:
#[allow(dead_code)]
pub struct OperatorConf<OP, Backend = DefaultBackend>
where
    OP: Operator + 'static,
    Backend: arcon_state::Backend,
{
    state_cons: Arc<dyn Fn(Arc<Backend>) -> OP::OperatorState + Send + Sync + 'static>,
    operator_cons: Arc<dyn Fn(Arc<Backend>) -> OP + Send + Sync + 'static>,
    state_id: StateID,
}

/*
impl<OP> Default for OperatorConf<OP, DefaultBackend>
where
    OP: Operator + 'static,
{
    fn default() -> Self {
        Self {
            /*
            state_cons: Some(Arc::new(|_| ())),
            operator_cons: None,
            state_id: format!("op_{}", uuid::Uuid::new_v4().to_string()),
            _marker: PhantomData,
            */
        }
    }
}
*/
/*

impl<A, OP, State, Backend> OperatorConf<A, OP, State, Backend>
where
    A: ArconType,
    OP: Operator + 'static,
    State: ArconState,
    Backend: arcon_state::Backend,
{
    pub fn new() -> Self {
        Self {
            state_cons: None,
            operator_cons: None,
            state_id: format!("op_{}", uuid::Uuid::new_v4().to_string()),
            _marker: PhantomData,
        }
    }

    pub fn set_operator_constructor(
        &mut self,
        f: impl Fn(Arc<Backend>) -> OP + Send + Sync + 'static,
    ) {
        self.operator_cons = Some(Arc::new(f));
    }
    pub fn set_state_constructor(
        &mut self,
        f: impl Fn(Arc<Backend>) -> State + Send + Sync + 'static,
    ) {
        self.state_cons = Some(Arc::new(f));
    }

    pub fn set_state_id(&mut self, state_id: impl Into<StateID>) {
        self.state_id = state_id.into();
    }
}

*/

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
