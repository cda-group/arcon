use super::Application;
use crate::{
    data::{ArconType, ArconMessage},
    stream::node::{debug::DebugNode, source::SourceEvent},
    application::{ArconLogger, conf::ApplicationConf, conf::ExecutionMode},
    manager::{
        epoch::{EpochManager, EpochEvent},
        snapshot::SnapshotManager,
    },
    control_plane::app::AppRegistration,
    dataflow::constructor::ErasedComponent,
};
use kompact::{prelude::{ActorRefFactory, Component, ActorPath, KompactSystem, ActorRefStrong, NamedPath}, component::AbstractComponent};
use std::sync::Arc;

/// An [`Application`] that has been fully assembled
pub struct AssembledApplication {
    pub(crate) app: Application,
    start_flag: bool,
    pub(crate) runtime: RuntimeComponents,
    // Type erased Arc<Component<DebugNode<A>>>
    pub(crate) debug_node: Option<ErasedComponent>,
    // Type erased Arc<dyn AbstractComponent<Message = ArconMessage<A>>>
    pub(crate) abstract_debug_node: Option<ErasedComponent>,
}

#[derive(Clone)]
pub struct RuntimeComponents {
    /// [`KompactSystem`] for Control Components
    pub(crate) ctrl_system: KompactSystem,
    /// [`KompactSystem`] for Data Processing Components
    pub(crate) data_system: KompactSystem,
    /// EpochManager component for this application
    pub(crate) epoch_manager: Option<Arc<Component<EpochManager>>>,
    /// SnapshotManager component for this application
    pub(crate) snapshot_manager: Arc<Component<SnapshotManager>>,
}

impl RuntimeComponents {
    /// Helper function to set up internals of the application
    #[allow(clippy::type_complexity)]
    pub(crate) fn new(arcon_conf: &ApplicationConf, logger: &ArconLogger) -> RuntimeComponents {
        let data_system = arcon_conf
            .data_system_conf()
            .build()
            .expect("KompactSystem");
        let ctrl_system = arcon_conf
            .ctrl_system_conf()
            .build()
            .expect("KompactSystem");

        let timeout = std::time::Duration::from_millis(500);

        let snapshot_manager = ctrl_system.create(SnapshotManager::new);

        let epoch_manager = match arcon_conf.execution_mode {
            ExecutionMode::Local => {
                let snapshot_manager_ref = snapshot_manager.actor_ref().hold().expect("fail");
                let epoch_manager = ctrl_system.create(|| {
                    EpochManager::new(
                        arcon_conf.epoch_interval,
                        snapshot_manager_ref,
                        logger.clone(),
                    )
                });
                ctrl_system
                    .start_notify(&epoch_manager)
                    .wait_timeout(timeout)
                    .expect("EpochManager comp never started!");

                Some(epoch_manager)
            }
            ExecutionMode::Distributed(_) => None,
        };

        ctrl_system
            .start_notify(&snapshot_manager)
            .wait_timeout(timeout)
            .expect("SnapshotManager comp never started!");

        RuntimeComponents{ctrl_system, data_system, snapshot_manager, epoch_manager}
    }
}

impl AssembledApplication {
    pub(crate) fn new(app: Application, runtime: RuntimeComponents) -> Self {
        Self {
            app,
            start_flag: false,
            runtime,
            debug_node: None,
            abstract_debug_node: None,
        }
    }

    /// Helper function to quickly set up a DefaultApplication (no Nodes/Pipeline defined)
    pub(crate) fn default() -> Self {
        let app = Application::default();
        let runtime = RuntimeComponents::new(&app.conf, &app.arcon_logger);
        AssembledApplication::new(app, runtime)
    }

    pub(crate) fn epoch_manager(&self) -> ActorRefStrong<EpochEvent> {
        if let Some(epoch_manager) = &self.runtime.epoch_manager {
            epoch_manager
                .actor_ref()
                .hold()
                .expect("Failed to fetch actor ref")
        } else {
            panic!(
                "Only local reference supported for now. should really be an ActorPath later on"
            );
        }
    }

    pub(crate) fn data_system(&mut self) -> &mut KompactSystem {
        &mut self.runtime.data_system
    }

    pub(crate) fn ctrl_system(&mut self) -> &mut KompactSystem {
        &mut self.runtime.ctrl_system
    }

    pub(crate) fn snapshot_manager(&mut self) -> &mut Arc<Component<SnapshotManager>> {
        &mut self.runtime.snapshot_manager
    }

    // NOTE: this function can be used while we are building up the dataflow.
    // Basically, we want to send information about this Arcon Process (conf.arcon_pid)
    // to the ControlPlane.
    pub(crate) fn _register_app(&mut self) {
        let source_manager: ActorPath = NamedPath::with_system(
            self.ctrl_system().system_path(),
            vec!["source_manager".into()],
        )
        .into();

        let _app = AppRegistration {
            name: self.app.conf.app_name.clone(),
            arcon_pids: vec![self.app.conf.arcon_pid], // TODO: all pids..
            sources: vec![source_manager.to_string()],
            pid: self.app.conf.arcon_pid,
        };
        // TODO: communicate with a component at the ControlPlane
    }


    /// Fetch DebugNode component of the [Application]
    ///
    /// Returns `None` if the [Application] was not configured with a DebugNode.
    /// Note that it is up to the user to make sure `A` is of correct type.
    pub(crate) fn get_debug_node<A: ArconType>(&self) -> Option<Arc<Component<DebugNode<A>>>> {
        self.debug_node.as_ref().map(|erased_comp| {
            erased_comp
                .clone()
                .downcast::<Component<DebugNode<A>>>()
                .unwrap()
        })
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
        if let Some(epoch_manager) = &self.runtime.epoch_manager {
            self.runtime
                .ctrl_system
                .start_notify(epoch_manager)
                .wait_timeout(std::time::Duration::from_millis(500))
                .expect("Failed to start EpochManager");
        }

        self.start_flag = true;
    }

    pub fn get_application_controller(&self) -> Option<ActorPath> {
        self.app.get_application_controller()
    }

    /// Awaits termination from the application
    ///
    /// Note that this blocks the current thread
    pub fn await_termination(self) {
        self.runtime.data_system.await_termination();
        self.runtime.ctrl_system.await_termination();
    }

    /// Shuts the application down and consumes the struct
    pub fn shutdown(self) {
        let _ = self.runtime.data_system.shutdown();
        let _ = self.runtime.ctrl_system.shutdown();
    }

    // internal helper to create a DebugNode from a Stream object
    pub(crate) fn create_debug_node<A>(&mut self, node: DebugNode<A>)
    where
        A: ArconType,
    {
        assert!(
            self.debug_node.is_none(),
            "DebugNode has already been created!"
        );
        let component = self.runtime.ctrl_system.create(|| node);

        self.runtime.ctrl_system
            .start_notify(&component)
            .wait_timeout(std::time::Duration::from_millis(500))
            .expect("DebugNode comp never started!");

        self.debug_node = Some(component.clone());
        // define abstract version of the component as the building phase needs it to downcast properly..
        let comp: Arc<dyn AbstractComponent<Message = ArconMessage<A>>> = component;
        self.abstract_debug_node = Some(Arc::new(comp) as ErasedComponent);
    }
}
