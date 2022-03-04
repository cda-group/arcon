//! The application module provides necessary utilties for creating an Arcon application.
//!
//! ## Application Configuration
//!
//! Each Arcon application must have an [ApplicationConf](conf::ApplicationConf) configured.
//! If you don't have any need to modify any parameter, then you can simply rely on the
//! defaults.
//!
//! ## Application Builder
//!
//! An [ApplicationBuilder](builder::ApplicationBuilder) holds the logical plan of an Arcon application
//! but also its configuration. You can use this struct to build an [Application].
//!
//! ### Usage
//! ```no_run
//! use arcon::prelude::*;
//!
//! let conf = ApplicationConf {
//!     watermark_interval: 2000,
//!     ..Default::default()
//! };
//!
//! let mut builder: ApplicationBuilder = (0..100u64)
//!     .to_stream(|conf| conf.set_arcon_time(ArconTime::Process))
//!     .map(|x| x * 10)
//!     .print()
//!     .builder();
//!
//! let mut app: Application = builder
//!     .config(conf)
//!     .build();
//!
//! app.run_and_block();
//! ```

#[cfg(all(feature = "metrics", not(feature = "prometheus_exporter")))]
use crate::metrics::log_recorder::LogRecorder;

use crate::{
    application::{conf::logger::ArconLogger, conf::ExecutionMode},
    buffer::event::PoolInfo,
    dataflow::constructor::{ErasedComponent, ErasedSourceManager},
    manager::{
        epoch::{EpochEvent, EpochManager},
        snapshot::SnapshotManager,
    },
    prelude::*,
    stream::node::{debug::DebugNode, source::SourceEvent},
};
use arcon_allocator::Allocator;
use std::sync::{Arc, Mutex};

pub mod builder;
pub mod conf;

pub use builder::ApplicationBuilder;
pub use conf::ApplicationConf;

#[cfg(all(feature = "prometheus_exporter", feature = "metrics", not(test)))]
use metrics_exporter_prometheus::PrometheusBuilder;

/// An Arcon Application
#[derive(Clone)]
pub struct Application {
    /// Configuration for this application
    pub(crate) conf: ApplicationConf,
    /// Arcon allocator for this application
    pub(crate) allocator: Arc<Mutex<Allocator>>,
    /// Flag indicating whether to spawn a debug node for the Application
    debug_node_flag: bool,
    /// Configured Logger for the Application
    pub(crate) arcon_logger: ArconLogger,
    start_flag: bool,
    pub(crate) runtime: Runtime,
    // Type erased Arc<Component<DebugNode<A>>>
    pub(crate) debug_node: Option<ErasedComponent>,
    // Type erased Arc<dyn AbstractComponent<Message = ArconMessage<A>>>
    pub(crate) abstract_debug_node: Option<ErasedComponent>,
    /// SourceManager component for this application
    pub(crate) source_manager: Option<ErasedSourceManager>,
    /// EpochManager component for this application
    pub(crate) epoch_manager: Option<Arc<Component<EpochManager>>>,
    /// SnapshotManager component for this application
    pub(crate) snapshot_manager: Arc<Component<SnapshotManager>>,
}

impl Default for Application {
    fn default() -> Self {
        let conf: ApplicationConf = Default::default();
        Self::new(conf)
    }
}

impl Application {
    /// Creates a new Application using the given ApplicationConf
    fn new(conf: ApplicationConf) -> Self {
        #[cfg(all(feature = "prometheus_exporter", feature = "metrics", not(test)))]
        {
            PrometheusBuilder::new()
                .install()
                .expect("failed to install Prometheus recorder")
        }

        let allocator = Arc::new(Mutex::new(Allocator::new(conf.allocator_capacity)));
        let arcon_logger = conf.arcon_logger();

        #[cfg(all(feature = "metrics", not(feature = "prometheus_exporter")))]
        {
            let recorder = LogRecorder {
                logger: arcon_logger.clone(),
            };
            if let Err(_) = metrics::set_boxed_recorder(Box::new(recorder)) {
                // for tests, ignore logging this message as it will try to set the recorder multiple times..
                #[cfg(not(test))]
                error!(arcon_logger, "metrics recorder has already been set");
            }
        }
        let timeout = std::time::Duration::from_millis(500);

        let runtime = Runtime::new(&conf, &arcon_logger);
        let snapshot_manager = runtime.ctrl_system.create(SnapshotManager::new);

        let epoch_manager = match conf.execution_mode {
            ExecutionMode::Local => {
                let snapshot_manager_ref = snapshot_manager.actor_ref().hold().expect("fail");
                let epoch_manager = runtime.ctrl_system.create(|| {
                    EpochManager::new(
                        conf.epoch_interval,
                        snapshot_manager_ref,
                        arcon_logger.clone(),
                    )
                });
                runtime
                    .ctrl_system
                    .start_notify(&epoch_manager)
                    .wait_timeout(timeout)
                    .expect("EpochManager comp never started!");

                Some(epoch_manager)
            }
            ExecutionMode::Distributed(_) => None,
        };

        runtime
            .ctrl_system
            .start_notify(&snapshot_manager)
            .wait_timeout(timeout)
            .expect("SnapshotManager comp never started!");

        Self {
            conf,
            allocator,
            debug_node_flag: false,
            arcon_logger,
            start_flag: false,
            runtime,
            debug_node: None,
            abstract_debug_node: None,
            source_manager: None,
            snapshot_manager,
            epoch_manager,
        }
    }

    /// Creates a new Application using the given ApplicationConf
    pub(crate) fn with_conf(conf: ApplicationConf) -> Self {
        Self::new(conf)
    }

    // Internal helper for creating PoolInfo for a ChannelStrategy
    pub(crate) fn get_pool_info(&self) -> PoolInfo {
        PoolInfo::new(
            self.conf.channel_batch_size,
            self.conf.buffer_pool_size,
            self.allocator.clone(),
        )
    }

    /// Give out a reference to the ApplicationConf of the application
    pub(crate) fn arcon_conf(&self) -> &ApplicationConf {
        &self.conf
    }

    pub(crate) fn with_debug_node(&mut self) {
        self.debug_node_flag = true;
    }

    pub fn debug_node_enabled(&self) -> bool {
        self.debug_node_flag
    }
    pub(crate) fn set_source_manager(&mut self, source_manager: ErasedSourceManager) {
        self.source_manager = Some(source_manager);
    }

    pub(crate) fn epoch_manager(&self) -> ActorRefStrong<EpochEvent> {
        if let Some(epoch_manager) = &self.epoch_manager {
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

    pub(crate) fn data_system(&self) -> &KompactSystem {
        &self.runtime.data_system
    }

    pub(crate) fn ctrl_system(&self) -> &KompactSystem {
        &self.runtime.ctrl_system
    }

    pub(crate) fn snapshot_manager(&self) -> &Arc<Component<SnapshotManager>> {
        &self.snapshot_manager
    }

    /// Fetch DebugNode component of the [Application]
    ///
    /// Returns `None` if the [Application] was not configured with a DebugNode.
    /// Note that it is up to the user to make sure `A` is of correct type.
    pub fn get_debug_node<A: ArconType>(&self) -> Option<Arc<Component<DebugNode<A>>>> {
        self.debug_node.as_ref().map(|erased_comp| {
            erased_comp
                .clone()
                .downcast::<Component<DebugNode<A>>>()
                .unwrap()
        })
    }

    /// Run the application and block until it terminates
    pub fn run_and_block(mut self) {
        self.start();
        self.await_termination();
    }

    /// Run the application without blocking
    ///
    /// Note that if this method called more than once, it will panic!
    pub fn run(&mut self) {
        self.start();
    }

    fn start(&mut self) {
        assert!(!self.start_flag, "The Application has already been started");

        // Send start message to manager component
        match &self.source_manager {
            Some(source_manager) => {
                source_manager.actor_ref().tell(SourceEvent::Start);
            }
            None => panic!("Something went wrong, no source manager has been created!"),
        }

        // Start epoch manager to begin the injection of epochs into the application.
        if let Some(epoch_manager) = &self.epoch_manager {
            self.runtime
                .ctrl_system
                .start_notify(epoch_manager)
                .wait_timeout(std::time::Duration::from_millis(500))
                .expect("Failed to start EpochManager");
        }
        self.start_flag = true;
    }

    /// Awaits termination from the application
    ///
    /// Note that this blocks the current thread
    fn await_termination(self) {
        self.runtime.data_system.await_termination();
        self.runtime.ctrl_system.await_termination();
    }

    /// Shuts the application down and consumes the struct
    pub fn shutdown(self) {
        let _ = self.runtime.data_system.shutdown();
        let _ = self.runtime.ctrl_system.shutdown();
    }
}

#[derive(Clone)]
pub(crate) struct Runtime {
    /// [`KompactSystem`] for Control Components
    pub(crate) ctrl_system: KompactSystem,
    /// [`KompactSystem`] for Data Processing Components
    pub(crate) data_system: KompactSystem,
}

impl Runtime {
    /// Helper function to set up internals of the application
    #[allow(clippy::type_complexity)]
    pub(crate) fn new(arcon_conf: &ApplicationConf, _logger: &ArconLogger) -> Runtime {
        let data_system = arcon_conf
            .data_system_conf()
            .build()
            .expect("KompactSystem");
        let ctrl_system = arcon_conf
            .ctrl_system_conf()
            .build()
            .expect("KompactSystem");

        Runtime {
            ctrl_system,
            data_system,
        }
    }
}
