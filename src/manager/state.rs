// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use super::snapshot::Snapshot;
use fxhash::FxHashMap;
use kompact::prelude::*;
use std::{collections::HashSet, sync::mpsc::Sender};

pub type StateID = String;

#[derive(Debug, Clone)]
pub enum StateEvent {
    Snapshot(StateID, Snapshot),
    Register(StateID),
}

pub struct StateManagerPort;

impl Port for StateManagerPort {
    type Indication = Never;
    type Request = StateEvent;
}

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
}

impl StateManager {
    pub fn new() -> Self {
        Self {
            ctx: ComponentContext::uninitialised(),
            manager_port: ProvidedPort::uninitialised(),
            registered_state_ids: HashSet::new(),
        }
    }
}

impl Provide<StateManagerPort> for StateManager {
    fn handle(&mut self, event: StateEvent) -> Handled {
        debug!(self.ctx.log(), "Got Event {:?}", event);

        match event {
            StateEvent::Snapshot(id, snapshot) => {}
            StateEvent::Register(id) => {}
        }
        Handled::Ok
    }
}

ignore_lifecycle!(StateManager);
