// Copyright (c) 2021, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::StateID,
    dataflow::conf::{DefaultBackend, OperatorConf, SourceConf},
    index::{ArconState, EMPTY_STATE_ID},
    stream::{
        operator::Operator,
        source::Source,
        time::{ArconTime, Time},
    },
};
use arcon_state::Backend;
use std::sync::Arc;

/// Operator Builder
///
/// Defines everything needed in order for Arcon to instantiate
/// and manage an Operator during runtime.
///
/// ```no_run
/// use arcon::prelude::*;
/// let builder = OperatorBuilder {
///    operator: Arc::new(|| Map::new(|x: u64| x + 10)),
///    state: Arc::new(|_backend: Arc<Sled>| EmptyState),
///    conf: Default::default(),
/// };
///```
#[derive(Clone)]
pub struct OperatorBuilder<OP: Operator, Backend = DefaultBackend> {
    /// Operator Constructor
    pub operator: Arc<dyn Fn() -> OP + Send + Sync + 'static>,
    /// State Constructor
    pub state: Arc<dyn Fn(Arc<Backend>) -> OP::OperatorState + Send + Sync + 'static>,
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

type SourceIndex = usize;
type TotalSources = usize;

pub enum SourceBuilderType<S, B>
where
    S: Source,
    B: Backend,
{
    Single(SourceBuilder<S, B>),
    Parallel(ParallelSourceBuilder<S, B>),
}

impl<S, B> SourceBuilderType<S, B>
where
    S: Source,
    B: Backend,
{
    pub fn parallelism(&self) -> usize {
        match self {
            SourceBuilderType::Single(_) => 1,
            SourceBuilderType::Parallel(builder) => builder.parallelism,
        }
    }
    pub fn time(&self) -> ArconTime {
        match self {
            SourceBuilderType::Single(builder) => builder.conf.time,
            SourceBuilderType::Parallel(builder) => builder.conf.time,
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
    pub conf: SourceConf<S::Item>,
}

#[derive(Clone)]
pub struct ParallelSourceBuilder<S: Source, Backend = DefaultBackend> {
    /// Source Constructor
    pub constructor:
        Arc<dyn Fn(Arc<Backend>, SourceIndex, TotalSources) -> S + Send + Sync + 'static>,
    /// Source Config
    pub conf: SourceConf<S::Item>,
    /// Source Parallleism
    pub parallelism: usize,
}

/// Enum containing different window assigner types
#[derive(Clone)]
pub enum Assigner {
    Sliding {
        length: Time,
        slide: Time,
        late_arrival: Time,
    },
    Tumbling {
        length: Time,
        late_arrival: Time,
    },
}
