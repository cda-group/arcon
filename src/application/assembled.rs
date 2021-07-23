// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use super::Application;
use crate::{
    data::ArconType,
    index::ArconState,
    manager::snapshot::Snapshot,
    stream::node::{debug::DebugNode, source::SourceEvent},
};
use kompact::{
    component::AbstractComponent,
    prelude::{ActorRefFactory, Component},
};
use std::sync::{
    mpsc,
    mpsc::{Receiver, Sender},
    Arc,
};

/// An [`Application`] that has been fully assembled
pub struct AssembledApplication {
    app: Application,
    start_flag: bool,
}

impl AssembledApplication {
    pub(crate) fn new(app: Application) -> Self {
        Self {
            app,
            start_flag: false,
        }
    }
}

impl AssembledApplication {
    /// Instructs the SourceManager of the application
    /// to inject a start message to the source components
    /// of the application.
    ///
    /// The function will panic if no sources have been created
    pub fn start(&mut self) {
        assert_ne!(
            self.start_flag, true,
            "The AssembledApplication has already been started"
        );

        // Send start message to manager component
        match &self.app.source_manager {
            Some(source_manager) => {
                source_manager.actor_ref().tell(SourceEvent::Start);
            }
            None => panic!("Something went wrong, no source manager has been created!"),
        }

        // Start epoch manager to begin the injection of epochs into the application.
        if let Some(epoch_manager) = &self.app.epoch_manager {
            self.app
                .ctrl_system
                .start_notify(epoch_manager)
                .wait_timeout(std::time::Duration::from_millis(500))
                .expect("Failed to start EpochManager");
        }

        self.start_flag = true;
    }

    /// Fetch DebugNode component of the [Application]
    ///
    /// Returns `None` if the [Application] was not configured with a DebugNode.
    /// Note that it is up to the user to make sure `A` is of correct type.
    pub fn get_debug_node<A>(&self) -> Option<Arc<Component<DebugNode<A>>>>
    where
        A: ArconType,
    {
        self.app.get_debug_node()
    }

    /// Awaits termination from the application
    ///
    /// Note that this blocks the current thread
    pub fn await_termination(self) {
        self.app.data_system.await_termination();
        self.app.ctrl_system.await_termination();
    }

    /// Shuts the application down and consumes the struct
    pub fn shutdown(self) {
        let _ = self.app.data_system.shutdown();
        let _ = self.app.ctrl_system.shutdown();
    }

    /// Spawns a new thread to run the function `F` on the ArconState `S` per epoch.
    pub fn watch<S, F>(&mut self, f: F)
    where
        S: ArconState,
        F: Fn(u64, S) + Send + Sync + 'static,
    {
        let (tx, rx): (Sender<Snapshot>, Receiver<Snapshot>) = mpsc::channel();
        std::thread::spawn(move || loop {
            let snapshot = rx.recv().unwrap();
            let epoch = snapshot.epoch;
            let state: S = S::restore(snapshot).unwrap();
            f(epoch, state);
        });

        self.app.snapshot_manager.on_definition(|cd| {
            let state_id = S::STATE_ID.to_owned();
            if !cd.registered_state_ids.contains(&state_id) {
                panic!(
                    "State id {} has not been registered at the StateManager",
                    state_id
                );
            }
            cd.channels.insert(state_id, tx);
        });
    }

    /// Add component `c` to receive state snapshots from `state_id`
    ///
    /// Note that it is up to the target component to convert the [`Snapshot`]
    /// into some meaningful state.
    pub fn watch_with<S: ArconState>(&mut self, c: Arc<dyn AbstractComponent<Message = Snapshot>>) {
        self.app.snapshot_manager.on_definition(|cd| {
            let state_id = S::STATE_ID.to_owned();

            if !cd.registered_state_ids.contains(&state_id) {
                panic!(
                    "State id {} has not been registered at the StateManager",
                    state_id
                );
            }

            let actor_ref = c.actor_ref().hold().expect("fail");

            cd.subscribers
                .entry(state_id)
                .or_insert_with(Vec::new)
                .push(actor_ref);
        });
    }
}
