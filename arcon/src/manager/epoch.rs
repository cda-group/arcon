use crate::{
    application::conf::logger::ArconLogger,
    data::{Epoch, StateID},
    stream::node::source::SourceEvent,
};
use kompact::prelude::*;
use std::collections::HashSet;

#[derive(Debug)]
pub struct EpochCommit(pub Epoch);

#[derive(Debug)]
pub enum EpochEvent {
    /// Acknowledgement that `StateID` has committed a checkpoint for epoch `Epoch`
    Ack(StateID, Epoch),
    Register(StateID),
    Halt,
}

/// Component that injects epoch makers into an Arcon Pipeline
#[derive(ComponentDefinition)]
pub struct EpochManager {
    /// Component Context
    ctx: ComponentContext<Self>,
    /// Next epoch to be injected
    next_epoch: u64,
    /// Interval in millis to schedule injection timer
    epoch_interval: u64,
    /// Reference to the SourceManager
    pub(crate) source_manager: Option<ActorRefStrong<SourceEvent>>,
    /// Kompact Timer
    epoch_timeout: Option<ScheduledTimer>,
    /// Set of known state ids the EpochManager expects acknowledgements from
    known_state_ids: HashSet<StateID>,
    /// The epoch that is currently in process of being comitted
    ongoing_epoch_commit: u64,
    /// Set of Acks for a commit process
    epoch_acks: HashSet<(StateID, Epoch)>,
    /// Actor Reference to the SnapshotManager
    snapshot_manager: ActorRefStrong<EpochCommit>,
    logger: ArconLogger,
}

impl EpochManager {
    pub fn new(
        epoch_interval: u64,
        snapshot_manager: ActorRefStrong<EpochCommit>,
        logger: ArconLogger,
    ) -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            next_epoch: 0,
            known_state_ids: HashSet::new(),
            epoch_acks: HashSet::new(),
            ongoing_epoch_commit: 0,
            epoch_interval,
            snapshot_manager,
            source_manager: None,
            epoch_timeout: None,
            logger,
        }
    }
    fn handle_timeout(&mut self, timeout_id: ScheduledTimer) -> Handled {
        match self.epoch_timeout {
            Some(ref timeout) if *timeout == timeout_id => {
                if let Some(source_manager) = &self.source_manager {
                    source_manager.tell(SourceEvent::Epoch(Epoch::new(self.next_epoch)));
                    self.next_epoch += 1;
                } else {
                    #[cfg(not(test))]
                    error!(self.logger, "SourceManager was never set");
                }
                Handled::Ok
            }
            Some(_) => Handled::Ok, // just ignore outdated timeouts
            None => {
                warn!(self.logger, "Got unexpected timeout: {:?}", timeout_id);
                Handled::Ok
            } // can happen during restart or teardown
        }
    }
    fn handle_epoch_event(&mut self, event: EpochEvent) {
        match event {
            EpochEvent::Ack(state_id, epoch) => {
                // verify the state_id
                if self.known_state_ids.contains(&state_id) {
                    // Make sure the epoch is for the ongoing checkpoint
                    if epoch.epoch == self.ongoing_epoch_commit {
                        self.epoch_acks.insert((state_id, epoch));
                        if self.epoch_acks.len() == self.known_state_ids.len() {
                            self.ongoing_epoch_commit = epoch.epoch + 1;
                            self.snapshot_manager.tell(EpochCommit(epoch));
                            self.epoch_acks.clear();
                        }
                    }
                } else {
                    info!(
                        self.logger,
                        "Ignoring EpochEvent from unknown StateID {}", state_id
                    );
                }
            }
            EpochEvent::Register(state_id) => {
                if self.known_state_ids.contains(&state_id) {
                    // TODO: make whole system shutdown?
                    panic!("State ID {} cannot be registered multiple times", state_id);
                } else {
                    self.known_state_ids.insert(state_id);
                }
            }
            EpochEvent::Halt => {
                if let Some(source_manager) = &self.source_manager {
                    // Send a final epoch marker before revoking the timer
                    info!(
                        self.logger,
                        "EpochManager sending final epoch marker {:?}", self.next_epoch
                    );
                    source_manager.tell(SourceEvent::Epoch(Epoch::new(self.next_epoch)));
                    if let Some(timeout) = self.epoch_timeout.take() {
                        self.cancel_timer(timeout);
                    }
                } else {
                    error!(self.logger, "SourceManager was never set");
                }
            }
        }
    }
}

impl Actor for EpochManager {
    type Message = EpochEvent;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        self.handle_epoch_event(msg);
        Handled::Ok
    }
    fn receive_network(&mut self, _: NetMessage) -> Handled {
        unimplemented!();
    }
}

impl ComponentLifecycle for EpochManager {
    fn on_start(&mut self) -> Handled {
        let duration = std::time::Duration::from_millis(self.epoch_interval);
        let timeout = self.schedule_periodic(duration, duration, Self::handle_timeout);
        self.epoch_timeout = Some(timeout);
        Handled::Ok
    }
    fn on_stop(&mut self) -> Handled {
        if let Some(timeout) = self.epoch_timeout.take() {
            self.cancel_timer(timeout);
        }
        Handled::Ok
    }
}
