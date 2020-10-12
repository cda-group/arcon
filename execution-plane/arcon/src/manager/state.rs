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
    epoch: u64,
    snapshot_path: String,
    backend_type: String,
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
}

impl StateManager {
    pub fn new() -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            manager_port: ProvidedPort::uninitialised(),
            catalog: FxHashMap::default(),
        }
    }
}

impl Provide<StateManagerPort> for StateManager {
    fn handle(&mut self, event: StateEvent) -> Handled {
        debug!(self.ctx.log(), "Got Event {:?}", event);

        match event {
            StateEvent::Snapshot(id, snapshot) => {
                self.catalog.insert(id, snapshot);
                /*
                if self.subscribers.contains(&id) {
                    //
                    // self.send SnapshotReference to subscriber
                }
                */
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
    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        unreachable!();
    }
    fn receive_network(&mut self, _: NetMessage) -> Handled {
        unreachable!();
    }
}
