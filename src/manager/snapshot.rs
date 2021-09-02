use super::epoch::EpochCommit;
use crate::data::{Epoch, StateID};
use fxhash::FxHashMap;
use kompact::prelude::*;
use std::{collections::HashSet, sync::mpsc::Sender};

#[derive(Debug, Clone)]
pub enum SnapshotEvent {
    Snapshot(StateID, Snapshot),
    /// Register StateID as an valid id to subscribe to
    Register(StateID),
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub epoch: u64,
    pub snapshot_path: String,
    pub backend_name: String,
}

impl Snapshot {
    pub fn new(backend_name: String, epoch: u64, snapshot_path: String) -> Self {
        Snapshot {
            epoch,
            snapshot_path,
            backend_name,
        }
    }
}

pub struct SnapshotManagerPort;

impl Port for SnapshotManagerPort {
    type Indication = Never;
    type Request = SnapshotEvent;
}

/// Component that keeps a catalog of snapshots of a given application
#[derive(ComponentDefinition)]
pub struct SnapshotManager {
    /// Component Context
    ctx: ComponentContext<Self>,
    /// Port for incoming events
    pub(crate) manager_port: ProvidedPort<SnapshotManagerPort>,
    /// Set of registerd state ids
    ///
    /// Used to verify that users do not watch for state ids that do not exist
    pub(crate) registered_state_ids: HashSet<StateID>,
    /// Snapshot catalog of committed snapshots
    uncommitted_catalog: FxHashMap<Epoch, FxHashMap<StateID, Snapshot>>,
    /// Snapshot catalog of uncommitted snapshots
    committed_catalog: FxHashMap<Epoch, FxHashMap<StateID, Snapshot>>,
    /// A map matching state ids to a channel Sender
    pub(crate) channels: FxHashMap<StateID, Sender<Snapshot>>,
    /// A map of component subscribers per State ID
    pub(crate) subscribers: FxHashMap<StateID, Vec<ActorRefStrong<Snapshot>>>,
}

impl SnapshotManager {
    pub fn new() -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            manager_port: ProvidedPort::uninitialised(),
            registered_state_ids: HashSet::new(),
            uncommitted_catalog: FxHashMap::default(),
            committed_catalog: FxHashMap::default(),
            channels: FxHashMap::default(),
            subscribers: FxHashMap::default(),
        }
    }

    fn handle_epoch_commit(&mut self, commit: EpochCommit) {
        let epoch = commit.0;

        if let Some(snapshot_map) = self.uncommitted_catalog.remove(&epoch) {
            for (state_id, snapshot) in &snapshot_map {
                // check for component subscribers
                if let Some(subscribers) = self.subscribers.get(state_id) {
                    for sub in subscribers {
                        sub.tell(snapshot.clone());
                    }
                }

                // check for channel subscriptions
                if let Some(channel) = self.channels.get(state_id) {
                    channel.send(snapshot.clone()).unwrap();
                }
            }
            // insert snapshot map into the committed catalog
            self.committed_catalog.insert(epoch, snapshot_map);
        }
    }
}

impl Actor for SnapshotManager {
    type Message = EpochCommit;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        self.handle_epoch_commit(msg);
        Handled::Ok
    }
    fn receive_network(&mut self, _: NetMessage) -> Handled {
        unimplemented!();
    }
}

impl Provide<SnapshotManagerPort> for SnapshotManager {
    fn handle(&mut self, event: SnapshotEvent) -> Handled {
        debug!(self.ctx.log(), "Got Event {:?}", event);

        match event {
            SnapshotEvent::Snapshot(id, snapshot) => {
                let epoch = Epoch::new(snapshot.epoch);
                let snapshot_map = self
                    .uncommitted_catalog
                    .entry(epoch)
                    .or_insert_with(FxHashMap::default);

                snapshot_map.insert(id, snapshot);
            }
            SnapshotEvent::Register(id) => {
                if self.registered_state_ids.contains(&id) {
                    // TODO: make whole system shutdown?
                    panic!("State ID {} cannot be registered multiple times", id);
                } else {
                    self.registered_state_ids.insert(id);
                }
            }
        }

        Handled::Ok
    }
}

ignore_lifecycle!(SnapshotManager);
