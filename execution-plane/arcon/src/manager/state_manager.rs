// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::data::{tree::*, Epoch};
use fxhash::FxHashMap;
use kompact::prelude::*;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub enum StateEvent {
    Update(StateUpdate),
}

#[derive(Debug, Clone)]
pub struct StateUpdate {
    pub(crate) key_id: KeyedStateID,
    pub(crate) key_range: KeyRange,
    pub(crate) epoch: Epoch,
}

pub struct StateManagerPort {}
impl Port for StateManagerPort {
    type Indication = ();
    type Request = StateEvent;
}

#[derive(Debug)]
pub enum ExecutionMode {
    Cluster,
    Single,
}

#[derive(Debug)]
pub enum Status {
    Leader,
    Follower,
}

/// A [kompact] component responsible for coordinating state
#[derive(ComponentDefinition)]
pub struct StateManager {
    /// Component context
    ctx: ComponentContext<Self>,
    /// Component Port for incoming local events
    manager_port: ProvidedPort<StateManagerPort, Self>,
    /// Other available StateManager's in the same region
    // TODO: perhaps should be ordered as closest neighbours by key range
    peers: Vec<ActorPath>,
    /// Current Leader in the region
    //leader: ActorPath,
    /// Available Merkle trees per KeyedStateID
    state_trees: FxHashMap<KeyedStateID, MerkleTree>,
    /// Execution Mode
    ///
    /// Single machine or cluster
    execution_mode: ExecutionMode,
    /// Current status of the StateManager
    ///
    /// Can either be a Leader or Follower
    status: Status,
    /// Controls the amount of replicaton done to other processes
    replication_factor: usize,
}

impl StateManager {
    /// Create a new StateManager
    pub fn new(mode: ExecutionMode) -> StateManager {
        let ctx = ComponentContext::<StateManager>::new();
        // ActorPath to one self
        //let actor_path = ctx.actor_path();
        StateManager {
            ctx: ComponentContext::new(),
            manager_port: ProvidedPort::new(),
            state_trees: FxHashMap::default(),
            peers: Vec::new(),
            //leader: actor_path,
            execution_mode: mode,
            status: Status::Follower,
            replication_factor: 2,
        }
    }
}

impl Provide<ControlPort> for StateManager {
    fn handle(&mut self, event: ControlEvent) {
        match event {
            ControlEvent::Start => {
                match self.execution_mode {
                    ExecutionMode::Cluster => {
                        // Connect with coordination service
                        // Find out leader
                        // modify self.leader
                    }
                    ExecutionMode::Single => {
                        // Keep self.leader as this component
                    }
                }
                info!(
                    self.ctx.log(),
                    "Started StateManager in {:?} mode with status of {:?}
                    ,using replication factor of {}",
                    self.execution_mode,
                    self.status,
                    self.replication_factor
                );
            }
            ControlEvent::Kill => {}
            ControlEvent::Stop => {}
        }
    }
}

impl Provide<StateManagerPort> for StateManager {
    fn handle(&mut self, event: StateEvent) {
        debug!(self.ctx.log(), "Got Event {:?}", event);
        match event {
            StateEvent::Update(update) => {
                if let Some(tree) = self.state_trees.get(&update.key_id) {
                    match self.status {
                        Status::Leader => {
                            // Update tree?
                        },
                        Status::Follower => {
                            // Send to leader
                        }
                    }
                } else {
                    error!(
                        self.ctx.log(),
                        "Was not able to find MerkleTree for KeyedStateID {:?}", update.key_id
                    );
                }
            }
        }
    }
}

impl Actor for StateManager {
    type Message = ();
    fn receive_local(&mut self, _: Self::Message) {}
    fn receive_network(&mut self, _: NetMessage) {}
}
