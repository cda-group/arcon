// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use super::Pipeline;
use crate::{
    manager::state::{SnapshotRef, StateID},
    stream::source::ArconSource,
};
use arcon_state::index::ArconState;
use kompact::{component::AbstractComponent, prelude::ActorRefFactory};
use std::sync::{
    mpsc,
    mpsc::{Receiver, Sender},
    Arc,
};

/// A [`Pipeline`] that has been fully assembled
pub struct AssembledPipeline {
    pipeline: Pipeline,
    start_flag: bool,
}

impl AssembledPipeline {
    pub(crate) fn new(pipeline: Pipeline) -> Self {
        Self {
            pipeline,
            start_flag: false,
        }
    }
}

impl AssembledPipeline {
    /// Instructs the SourceManager of the pipeline
    /// to inject a start message to the source components
    /// of the pipeline.
    ///
    /// The function will panic if no sources have been created
    pub fn start(&mut self) {
        assert_ne!(
            self.start_flag, true,
            "The AssembledPipeline has already been started"
        );

        assert_ne!(
            self.pipeline
                .source_manager
                .on_definition(|cd| cd.sources.len()),
            0,
            "No source components have been created, cannot start the pipeline!"
        );

        // Send start message to manager component
        self.pipeline
            .source_manager
            .actor_ref()
            .tell(ArconSource::Start);

        // Start epoch manager to begin the injection of epochs into the pipeline.
        if let Some(epoch_manager) = &self.pipeline.epoch_manager {
            self.pipeline
                .ctrl_system
                .start_notify(&epoch_manager)
                .wait_timeout(std::time::Duration::from_millis(500))
                .expect("Failed to start EpochManager");
        }

        self.start_flag = true;
    }

    /// Awaits termination from the pipeline
    ///
    /// Note that this blocks the current thread
    pub fn await_termination(self) {
        self.pipeline.data_system.await_termination();
        self.pipeline.ctrl_system.await_termination();
    }

    /// Shuts the pipeline down and consumes the struct
    pub fn shutdown(self) {
        let _ = self.pipeline.data_system.shutdown();
        let _ = self.pipeline.ctrl_system.shutdown();
    }

    /// Spawns a new thread to run the function `F` on the ArconState `S` per epoch.
    pub fn watch<S, F>(&mut self, state_id: impl Into<StateID>, f: F)
    where
        S: ArconState + std::convert::From<SnapshotRef>,
        F: Fn(u64, S) + Send + Sync + 'static,
    {
        let (tx, rx): (Sender<SnapshotRef>, Receiver<SnapshotRef>) = mpsc::channel();
        std::thread::spawn(move || loop {
            let snapshot_ref = rx.recv().unwrap();
            let epoch = snapshot_ref.snapshot.epoch;
            let state: S = snapshot_ref.into();
            f(epoch, state);
        });

        self.pipeline.state_manager.on_definition(|cd| {
            let state_id = state_id.into();
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
    /// Note that it is up to the target component to convert the [`SnapshotRef`]
    /// into some meaningful state.
    pub fn watch_with(
        &mut self,
        state_ids: Vec<impl Into<StateID>>,
        c: Arc<dyn AbstractComponent<Message = SnapshotRef>>,
    ) {
        self.pipeline.state_manager.on_definition(|cd| {
            for id in state_ids.into_iter() {
                let state_id = id.into();

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
            }
        });
    }
}
