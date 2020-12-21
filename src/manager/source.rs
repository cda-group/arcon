// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use super::epoch::EpochEvent;
use crate::{data::StateID, stream::node::source::SourceEvent};
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
    watermark_interval: u64,
    /// Kompact Timer
    watermark_timeout: Option<ScheduledTimer>,
    state_id: StateID,
    /// Vector of source components
    ///
    /// May contain more than 1 component if the source supports parallelism
    pub(crate) sources: Vec<Arc<dyn AbstractComponent<Message = SourceEvent>>>,
    /// A shared backend for sources
    _backend: Arc<B>,
    /// Reference to the EpochManager
    _epoch_manager: ActorRefStrong<EpochEvent>,
}

impl<B: Backend> SourceManager<B> {
    pub fn new(
        state_id: StateID,
        watermark_interval: u64,
        sources: Vec<Arc<dyn AbstractComponent<Message = SourceEvent>>>,
        epoch_manager: ActorRefStrong<EpochEvent>,
        backend: Arc<B>,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            watermark_interval,
            watermark_timeout: None,
            state_id,
            sources,
            _backend: backend,
            _epoch_manager: epoch_manager,
        }
    }

    fn handle_watermark_timeout(&mut self, timeout_id: ScheduledTimer) -> Handled {
        match self.watermark_timeout {
            Some(ref timeout) if *timeout == timeout_id => {
                for source in &self.sources {
                    source.actor_ref().tell(SourceEvent::Watermark);
                }
                Handled::Ok
            }
            Some(_) => Handled::Ok, // just ignore outdated timeouts
            None => {
                warn!(self.log(), "Got unexpected timeout: {:?}", timeout_id);
                Handled::Ok
            } // can happen during restart or teardown
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
    fn on_stop(&mut self) -> Handled {
        if let Some(timeout) = self.watermark_timeout.take() {
            self.cancel_timer(timeout);
        }
        Handled::Ok
    }
}

impl<B: Backend> Actor for SourceManager<B> {
    type Message = SourceEvent;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        // If we received a start message, start the periodic timer
        // that instructs sources to send off watermarks.
        if SourceEvent::Start == msg {
            let duration = std::time::Duration::from_millis(self.watermark_interval);
            let timeout =
                self.schedule_periodic(duration, duration, Self::handle_watermark_timeout);
            self.watermark_timeout = Some(timeout);
        }

        for source in &self.sources {
            source.actor_ref().tell(msg.clone());
        }

        Handled::Ok
    }
    fn receive_network(&mut self, _: NetMessage) -> Handled {
        unimplemented!();
    }
}
