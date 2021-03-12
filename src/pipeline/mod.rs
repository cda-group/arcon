// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

#[cfg(feature = "arcon_arrow")]
use crate::manager::query::{QueryManager, QUERY_MANAGER_NAME};
use crate::{
    buffer::event::PoolInfo,
    conf::{ArconConf, ExecutionMode},
    dataflow::{
        conf::{DefaultBackend, SourceBuilder, SourceConf},
        constructor::source_manager_constructor,
        dfg::*,
        stream::Context,
    },
    manager::{
        endpoint::{EndpointManager, ENDPOINT_MANAGER_NAME},
        epoch::{EpochEvent, EpochManager},
        snapshot::SnapshotManager,
    },
    prelude::*,
    stream::{
        node::source::SourceEvent,
        source::{collection::CollectionSource, local_file::LocalFileSource, Source},
    },
};
use arcon_allocator::Allocator;
use kompact::{component::AbstractComponent, prelude::KompactSystem};
use std::sync::{Arc, Mutex};

mod assembled;

pub use crate::dataflow::stream::Stream;
pub use assembled::AssembledPipeline;

/// A Pipeline is the starting point of all Arcon applications.
/// It contains all necessary runtime components, configuration,
/// and a custom allocator.
///
/// # Creating a Pipeline
///
/// See [Configuration](ArconConf)
///
/// With the default configuration
/// ```
/// use arcon::prelude::Pipeline;
///
/// let pipeline = Pipeline::default();
/// ```
///
/// With configuration
/// ```
/// use arcon::prelude::{Pipeline, ArconConf};
///
/// let conf = ArconConf {
///     watermark_interval: 2000,
///     ..Default::default()
/// };
/// let pipeline = Pipeline::with_conf(conf);
/// ```
#[derive(Clone)]
pub struct Pipeline {
    /// [`KompactSystem`] for Control Components
    pub(crate) ctrl_system: KompactSystem,
    /// [`KompactSystem`] for Data Processing Components
    pub(crate) data_system: KompactSystem,
    /// Arcon configuration for this pipeline
    pub(crate) conf: ArconConf,
    /// Arcon allocator for this pipeline
    pub(crate) allocator: Arc<Mutex<Allocator>>,
    /// SourceManager component for this pipeline
    pub(crate) source_manager: Option<Arc<dyn AbstractComponent<Message = SourceEvent>>>,
    /// EpochManager component for this pipeline
    pub(crate) epoch_manager: Option<Arc<Component<EpochManager>>>,
    /// SnapshotManager component for this pipeline
    pub(crate) snapshot_manager: Arc<Component<SnapshotManager>>,
    endpoint_manager: Arc<Component<EndpointManager>>,
    #[cfg(feature = "arcon_arrow")]
    pub(crate) query_manager: Arc<Component<QueryManager>>,
}

impl Default for Pipeline {
    fn default() -> Self {
        let conf: ArconConf = Default::default();
        Self::new(conf)
    }
}

impl Pipeline {
    /// Creates a new Pipeline using the given ArconConf
    fn new(conf: ArconConf) -> Self {
        let allocator = Arc::new(Mutex::new(Allocator::new(conf.allocator_capacity)));
        let (ctrl_system, data_system, snapshot_manager, epoch_manager) = Self::setup(&conf);
        let endpoint_manager = ctrl_system.create(EndpointManager::new);
        #[cfg(feature = "arcon_arrow")]
        let query_manager = ctrl_system.create(QueryManager::new);

        let timeout = std::time::Duration::from_millis(500);

        ctrl_system
            .start_notify(&endpoint_manager)
            .wait_timeout(timeout)
            .expect("EndpointManager comp never started!");

        #[cfg(feature = "arcon_arrow")]
        ctrl_system
            .start_notify(&query_manager)
            .wait_timeout(timeout)
            .expect("QueryManager comp never started!");

        #[cfg(feature = "arcon_arrow")]
        biconnect_components(&query_manager, epoch_manager.as_ref().unwrap())
            .expect("Failed to connect EpochManager and QueryManager");

        if conf.ctrl_system_host.is_some() {
            ctrl_system
                .register_by_alias(&endpoint_manager, ENDPOINT_MANAGER_NAME)
                .wait_expect(timeout, "Registration never completed.");

            #[cfg(feature = "arcon_arrow")]
            ctrl_system
                .register_by_alias(&query_manager, QUERY_MANAGER_NAME)
                .wait_expect(timeout, "Registration never completed.");
        }

        Self {
            ctrl_system,
            data_system,
            conf,
            allocator,
            snapshot_manager,
            epoch_manager,
            source_manager: None,
            endpoint_manager,
            #[cfg(feature = "arcon_arrow")]
            query_manager,
        }
    }

    /// Creates a new Pipeline using the given ArconConf
    pub fn with_conf(conf: ArconConf) -> Self {
        Self::new(conf)
    }

    /// Helper function to set up internals of the pipeline
    #[allow(clippy::type_complexity)]
    fn setup(
        arcon_conf: &ArconConf,
    ) -> (
        KompactSystem,
        KompactSystem,
        Arc<Component<SnapshotManager>>,
        Option<Arc<Component<EpochManager>>>,
    ) {
        let data_system = arcon_conf
            .data_system_conf()
            .build()
            .expect("KompactSystem");
        let ctrl_system = arcon_conf
            .ctrl_system_conf()
            .build()
            .expect("KompactSystem");

        let snapshot_manager = ctrl_system.create(SnapshotManager::new);

        let epoch_manager =
            match arcon_conf.execution_mode {
                ExecutionMode::Local => {
                    let snapshot_manager_ref = snapshot_manager.actor_ref().hold().expect("fail");
                    Some(ctrl_system.create(|| {
                        EpochManager::new(arcon_conf.epoch_interval, snapshot_manager_ref)
                    }))
                }
                ExecutionMode::Distributed(_) => None,
            };

        let timeout = std::time::Duration::from_millis(500);

        ctrl_system
            .start_notify(&snapshot_manager)
            .wait_timeout(timeout)
            .expect("SnapshotManager comp never started!");

        (ctrl_system, data_system, snapshot_manager, epoch_manager)
    }

    /// Create a non-parallel data source
    ///
    /// Returns a [`Stream`] object that users may execute transformations on.
    pub fn source<S>(self, builder: SourceBuilder<S>) -> Stream<S::Data>
    where
        S: Source,
    {
        assert_ne!(
            builder.conf.time == ArconTime::Event,
            builder.conf.extractor.is_none(),
            "Cannot use ArconTime::Event without specifying a timestamp extractor"
        );

        let mut state_dir = self.arcon_conf().state_dir.clone();
        state_dir.push("source_manager");
        let backend = Arc::new(DefaultBackend::create(&state_dir).unwrap());
        let time = builder.conf.time;
        let manager_constructor = source_manager_constructor::<S, _>(
            String::from("source_manager"),
            builder,
            backend,
            self.arcon_conf().watermark_interval,
            time,
        );
        let mut ctx = Context::new(self);
        let kind = DFGNodeKind::Source(Default::default(), manager_constructor);
        let incoming_channels = 0; // sources have 0 incoming channels..
        let outgoing_channels = 1; // TODO
        let dfg_node = DFGNode::new(kind, outgoing_channels, incoming_channels, vec![]);
        ctx.dfg.insert(dfg_node);
        Stream::new(ctx)
    }

    /// Creates a bounded data Stream using a local file
    ///
    /// Returns a [`Stream`] object that users may execute transformations on.
    ///
    /// Example
    /// ```no_run
    /// use arcon::prelude::*;
    /// let stream: Stream<u64> = Pipeline::default()
    ///     .file("/tmp/source_file", |conf| {
    ///         conf.set_arcon_time(ArconTime::Process);
    ///     });
    /// ```
    pub fn file<I, A>(self, i: I, f: impl FnOnce(&mut SourceConf<A>)) -> Stream<A>
    where
        I: Into<String>,
        A: ArconType + std::str::FromStr,
    {
        let path = i.into();
        assert_eq!(
            std::path::Path::new(&path).exists(),
            true,
            "File {} does not exist",
            path
        );
        let mut conf = SourceConf::default();
        f(&mut conf);

        let conf_copy = conf.clone();
        let builder = SourceBuilder {
            constructor: Arc::new(move |_| LocalFileSource::new(path.clone(), conf.clone())),
            conf: conf_copy,
        };

        self.source(builder)
    }

    /// Creates a bounded data Stream using a Collection
    ///
    /// Returns a [`Stream`] object that users may execute transformations on.
    ///
    /// Example
    /// ```
    /// use arcon::prelude::*;
    /// let stream: Stream<u64> = Pipeline::default()
    ///     .collection((0..100).collect::<Vec<u64>>(), |conf| {
    ///         conf.set_arcon_time(ArconTime::Process);
    ///     });
    /// ```
    pub fn collection<I, A>(self, i: I, f: impl FnOnce(&mut SourceConf<A>)) -> Stream<A>
    where
        I: Into<Vec<A>>,
        A: ArconType,
    {
        let collection = i.into();
        let mut conf = SourceConf::default();
        f(&mut conf);

        let conf_copy = conf.clone();

        let builder = SourceBuilder {
            constructor: Arc::new(move |_| CollectionSource::new(collection.clone(), conf.clone())),
            conf: conf_copy,
        };
        self.source(builder)
    }

    // Internal helper for creating PoolInfo for a ChannelStrategy
    pub(crate) fn get_pool_info(&self) -> PoolInfo {
        PoolInfo::new(
            self.conf.channel_batch_size,
            self.conf.buffer_pool_size,
            self.conf.buffer_pool_limit,
            self.allocator.clone(),
        )
    }

    // TODO: Remove
    pub fn shutdown(self) {
        let _ = self.data_system.shutdown();
        let _ = self.ctrl_system.shutdown();
    }

    pub(crate) fn data_system(&mut self) -> &mut KompactSystem {
        &mut self.data_system
    }

    pub(crate) fn ctrl_system(&mut self) -> &mut KompactSystem {
        &mut self.ctrl_system
    }

    /// Give out a reference to the ArconConf of the pipeline
    pub(crate) fn arcon_conf(&self) -> &ArconConf {
        &self.conf
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
}
