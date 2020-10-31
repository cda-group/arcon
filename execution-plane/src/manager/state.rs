// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use fxhash::FxHashMap;
use kompact::prelude::*;

pub type StateID = String;

#[derive(Debug, Clone)]
pub enum StateEvent {
    Snapshot(StateID, Snapshot),
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

#[derive(ComponentDefinition)]
pub struct StateManager {
    /// Component Context
    ctx: ComponentContext<Self>,
    /// Port for incoming events
    manager_port: ProvidedPort<StateManagerPort>,
    /// Snapshot Catalog
    catalog: FxHashMap<StateID, Snapshot>,
    /// A map of subscribers per State ID
    pub(crate) subscribers: FxHashMap<StateID, Vec<ActorRefStrong<SnapshotRef>>>,
}

impl StateManager {
    pub fn new() -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            manager_port: ProvidedPort::uninitialised(),
            catalog: FxHashMap::default(),
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
                self.catalog.insert(id, snapshot);
            }
        }
        Handled::Ok
    }
}

impl ComponentLifecycle for StateManager {
    fn on_start(&mut self) -> Handled {
        Handled::Ok
    }
}

impl Actor for StateManager {
    type Message = ();
    fn receive_local(&mut self, _: Self::Message) -> Handled {
        unreachable!();
    }
    fn receive_network(&mut self, _: NetMessage) -> Handled {
        unreachable!();
    }
}
