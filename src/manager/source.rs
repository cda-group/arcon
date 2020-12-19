// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{
    epoch::EpochEvent,
    snapshot::{SnapshotEvent, SnapshotManagerPort},
};
use crate::{data::StateID, stream::source::SourceEvent};
use arcon_error::ArconResult;
use arcon_state::Backend;
use kompact::{component::AbstractComponent, prelude::*};
use std::sync::Arc;

pub struct SourceManagerPort;
impl Port for SourceManagerPort {
    type Indication = Never;
    type Request = Never;
}

/// Component that manages a set of Arcon sources
#[derive(ComponentDefinition)]
pub(crate) struct SourceManager<B: Backend> {
    /// Component Context
    ctx: ComponentContext<Self>,
    state_id: StateID,
    /// Vector of source components
    ///
    /// May contain more than 1 component if the source supports parallelism
    pub(crate) sources: Vec<Arc<dyn AbstractComponent<Message = SourceEvent>>>,
    /// A shared backend for sources
    backend: Arc<B>,
    /// Port to the SnapshotManager
    snapshot_manager_port: RequiredPort<SnapshotManagerPort>,
    /// Reference to the EpochManager
    epoch_manager: ActorRefStrong<EpochEvent>,
}

impl<B: Backend> SourceManager<B> {
    pub fn new(
        state_id: StateID,
        sources: Vec<Arc<dyn AbstractComponent<Message = SourceEvent>>>,
        epoch_manager: ActorRefStrong<EpochEvent>,
        backend: Arc<B>,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            state_id,
            sources,
            backend,
            snapshot_manager_port: RequiredPort::uninitialised(),
            epoch_manager,
        }
    }
}

impl<B: Backend> ComponentLifecycle for SourceManager<B> {
    fn on_start(&mut self) -> Handled {
        info!(
            self.ctx.log(),
            "Started SourceManager for {}", self.state_id,
        );
        Handled::Ok
    }
}

impl<B> Require<SnapshotManagerPort> for SourceManager<B>
where
    B: Backend,
{
    fn handle(&mut self, _: Never) -> Handled {
        unreachable!("Never can't be instantiated!");
    }
}

impl<B> Provide<SnapshotManagerPort> for SourceManager<B>
where
    B: Backend,
{
    fn handle(&mut self, _: SnapshotEvent) -> Handled {
        Handled::Ok
    }
}

impl<B: Backend> Actor for SourceManager<B> {
    type Message = SourceEvent;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        for source in &self.sources {
            source.actor_ref().tell(msg.clone());
        }
        Handled::Ok
    }
    fn receive_network(&mut self, _: NetMessage) -> Handled {
        unimplemented!();
    }
}
