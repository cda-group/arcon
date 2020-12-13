// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use fxhash::FxHashMap;
use kompact::prelude::*;
use std::{collections::HashSet, sync::mpsc::Sender};

pub type StateID = String;

#[derive(Debug, Clone)]
pub enum StateEvent {
    Snapshot(StateID, Snapshot),
    Register(StateID),
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub epoch: u64,
    pub snapshot_path: String,
}

impl Snapshot {
    pub fn new(epoch: u64, snapshot_path: String) -> Self {
        Snapshot {
            epoch,
            snapshot_path,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SnapshotRef {
    pub state_id: StateID,
    pub snapshot: Snapshot,
}

pub struct StateManagerPort {}

impl Port for StateManagerPort {
    type Indication = Never;
    type Request = StateEvent;
}

/// State Manager Component that keeps a catalog of snapshots of a given pipeline
#[derive(ComponentDefinition, Actor)]
pub struct StateManager {
    /// Component Context
    ctx: ComponentContext<Self>,
    /// Port for incoming events
    pub(crate) manager_port: ProvidedPort<StateManagerPort>,
    /// Set of registerd state ids
    ///
    /// Used to verify that users do not watch for state ids that do not exist
    pub(crate) registered_state_ids: HashSet<StateID>,
    /// Snapshot Catalog
    catalog: FxHashMap<StateID, Snapshot>,
    pub(crate) channels: FxHashMap<StateID, Sender<SnapshotRef>>,
    /// A map of component subscribers per State ID
    pub(crate) subscribers: FxHashMap<StateID, Vec<ActorRefStrong<SnapshotRef>>>,
}

impl StateManager {
    pub fn new() -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            manager_port: ProvidedPort::uninitialised(),
            registered_state_ids: HashSet::new(),
            catalog: FxHashMap::default(),
            channels: FxHashMap::default(),
            subscribers: FxHashMap::default(),
        }
    }
}

impl Provide<StateManagerPort> for StateManager {
    fn handle(&mut self, event: StateEvent) -> Handled {
        debug!(self.ctx.log(), "Got Event {:?}", event);

        match event {
            StateEvent::Snapshot(id, snapshot) => {
                if let Some(subscribers) = self.subscribers.get(&id) {
                    let state_ref = SnapshotRef {
                        state_id: id.clone(),
                        snapshot: snapshot.clone(),
                    };
                    for sub in subscribers {
                        sub.tell(state_ref.clone());
                    }
                }
                if let Some(channel) = self.channels.get(&id) {
                    let state_ref = SnapshotRef {
                        state_id: id.clone(),
                        snapshot: snapshot.clone(),
                    };
                    channel.send(state_ref).unwrap();
                }
                self.catalog.insert(id, snapshot);
            }
            StateEvent::Register(id) => {
                self.registered_state_ids.insert(id);
            }
        }
        Handled::Ok
    }
}

ignore_lifecycle!(StateManager);
