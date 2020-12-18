// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{
    epoch::EpochEvent,
    snapshot::{SnapshotEvent, SnapshotManagerPort},
};
use crate::{data::StateID, stream::source::ArconSource};
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
    pub(crate) sources: Vec<Arc<dyn AbstractComponent<Message = ArconSource>>>,
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
        sources: Vec<Arc<dyn AbstractComponent<Message = ArconSource>>>,
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
    #[inline]
    fn checkpoint(&mut self) -> ArconResult<()> {
        /*
        if let Some(base_dir) = &self.ctx.config()["checkpoint_dir"].as_string() {
            let curr_epoch = self.manager_state.current_epoch().get().unwrap().epoch;

            let checkpoint_dir = format!(
                "{}/checkpoint_{id}_{epoch}",
                base_dir,
                id = self.state_id,
                epoch = curr_epoch,
            );

            self.backend.checkpoint(checkpoint_dir.as_ref())?;

            // Send snapshot to SnapshotManager
            self.snapshot_manager_port.trigger(SnapshotEvent::Snapshot(
                self.state_id.clone(),
                Snapshot::new(curr_epoch, checkpoint_dir.clone()),
            ));

            // Send Ack to EpochManager
            self.epoch_manager.tell(EpochEvent::Ack(
                self.state_id.clone(),
                Epoch::new(curr_epoch),
            ));

            // bump epoch
            self.manager_state.current_epoch().rmw(|e| {
                e.epoch += 1;
            });

            debug!(
                self.ctx.log(),
                "Completed a Checkpoint to path {}", checkpoint_dir
            );
        } else {
            return arcon_err!("Failed to fetch checkpoint_dir from Config");
        }
            */

        Ok(())
    }
}

impl<B: Backend> ComponentLifecycle for SourceManager<B> {
    fn on_start(&mut self) -> Handled {
        info!(
            self.ctx.log(),
            "Started SourceManager for {}", self.state_id,
        );
        // Register state id
        self.snapshot_manager_port
            .trigger(SnapshotEvent::Register(self.state_id.clone()));
        self.epoch_manager
            .tell(EpochEvent::Register(self.state_id.clone()));
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
    type Message = ArconSource;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        match msg {
            ArconSource::Epoch(_) => {
                for source in &self.sources {
                    source.actor_ref().tell(msg.clone());
                }
            }
            ArconSource::Start => {
                for source in &self.sources {
                    source.actor_ref().tell(msg.clone());
                }
            }
        }
        Handled::Ok
    }
    fn receive_network(&mut self, _: NetMessage) -> Handled {
        unimplemented!();
    }
}
