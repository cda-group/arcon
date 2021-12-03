use super::Application;
use crate::{
    data::ArconType,
    stream::node::{debug::DebugNode, source::SourceEvent},
};
use kompact::prelude::{ActorRefFactory, Component};
use std::sync::Arc;

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
        assert!(
            !self.start_flag,
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
}
