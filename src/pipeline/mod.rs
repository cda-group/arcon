// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

#[cfg(feature = "kafka")]
use crate::stream::source::{
    kafka::{KafkaConsumer, KafkaConsumerConf, KafkaConsumerState},
    schema::SourceSchema,
};
use crate::{
    buffer::event::PoolInfo,
    conf::{logger::ArconLogger, ArconConf, ExecutionMode},
    data::ArconMessage,
    dataflow::{
        api::{ParallelSourceBuilder, SourceBuilder, SourceBuilderType},
        conf::SourceConf,
        constructor::{source_manager_constructor, ErasedComponent},
        dfg::*,
        stream::Context,
    },
    manager::{
        endpoint::{EndpointManager, ENDPOINT_MANAGER_NAME},
        epoch::{EpochEvent, EpochManager},
        query::{QueryManager, QUERY_MANAGER_NAME},
        snapshot::SnapshotManager,
    },
    prelude::*,
    stream::{
        node::{debug::DebugNode, source::SourceEvent},
        source::{local_file::LocalFileSource, Source},
    },
};
use arcon_allocator::Allocator;
use kompact::{component::AbstractComponent, prelude::KompactSystem};
use std::sync::{Arc, Mutex};

mod assembled;

pub use crate::dataflow::stream::Stream;
pub use assembled::AssembledPipeline;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusRecorder};

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
    pub(crate) query_manager: Arc<Component<QueryManager>>,
    /// Flag indicating whether to spawn a debug node for the Pipeline
    debug_node_flag: bool,
    // Type erased Arc<Component<DebugNode<A>>>
    pub(crate) debug_node: Option<ErasedComponent>,
    // Type erased Arc<dyn AbstractComponent<Message = ArconMessage<A>>>
    pub(crate) abstract_debug_node: Option<ErasedComponent>,
    /// Configured Logger for the Pipeline
    pub(crate) arcon_logger: ArconLogger,
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

        let builder = PrometheusBuilder::new();
        builder
            .install()
            .expect("failed to install Prometheus recorder");



        let allocator = Arc::new(Mutex::new(Allocator::new(conf.allocator_capacity)));
        let arcon_logger = conf.arcon_logger();
        let (ctrl_system, data_system, snapshot_manager, epoch_manager) =
            Self::setup(&conf, &arcon_logger);
        let endpoint_manager = ctrl_system.create(EndpointManager::new);
        let query_manager = ctrl_system.create(QueryManager::new);

        let timeout = std::time::Duration::from_millis(500);

        ctrl_system
            .start_notify(&endpoint_manager)
            .wait_timeout(timeout)
            .expect("EndpointManager comp never started!");

        ctrl_system
            .start_notify(&query_manager)
            .wait_timeout(timeout)
            .expect("QueryManager comp never started!");

        biconnect_components(&query_manager, epoch_manager.as_ref().unwrap())
            .expect("Failed to connect EpochManager and QueryManager");

        if conf.ctrl_system_host.is_some() {
            ctrl_system
                .register_by_alias(&endpoint_manager, ENDPOINT_MANAGER_NAME)
                .wait_expect(timeout, "Registration never completed.");

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
            query_manager,
            debug_node_flag: false,
            debug_node: None,
            abstract_debug_node: None,
            arcon_logger,
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
        logger: &ArconLogger,
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

        (ctrl_system, data_system, snapshot_manager, epoch_manager)
    }

    /// Create a parallel data source
    ///
    /// Returns a [`Stream`] object that users may execute transformations on.
    pub fn parallel_source<S>(self, builder: ParallelSourceBuilder<S>) -> Stream<S::Item>
    where
        S: Source,
    {
        self.source_to_stream(SourceBuilderType::Parallel(builder))
    }

    /// Create a non-parallel data source
    ///
    /// Returns a [`Stream`] object that users may execute transformations on.
    pub fn source<S>(self, builder: SourceBuilder<S>) -> Stream<S::Item>
    where
        S: Source,
    {
        self.source_to_stream(SourceBuilderType::Single(builder))
    }

    fn source_to_stream<S, B>(self, builder_type: SourceBuilderType<S, B>) -> Stream<S::Item>
    where
        S: Source,
        B: Backend,
    {
        let parallelism = builder_type.parallelism();

        let mut state_dir = self.arcon_conf().state_dir();
        state_dir.push("source_manager");
        let backend = Arc::new(B::create(&state_dir).unwrap());
        let time = builder_type.time();
        let manager_constructor = source_manager_constructor::<S, B>(
            String::from("source_manager"),
            builder_type,
            backend,
            self.arcon_conf().watermark_interval,
            time,
        );
        let mut ctx = Context::new(self);
        let kind = DFGNodeKind::Source(Default::default(), manager_constructor);
        let incoming_channels = 0; // sources have 0 incoming channels..
        let outgoing_channels = parallelism;
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
        A: ArconType + std::str::FromStr + std::fmt::Display,
        <A as std::str::FromStr>::Err: std::fmt::Display,
    {
        let path = i.into();
        assert!(
            std::path::Path::new(&path).exists(),
            "File {} does not exist",
            path
        );
        let mut conf = SourceConf::default();
        f(&mut conf);

        let builder = SourceBuilder {
            constructor: Arc::new(move |_| LocalFileSource::new(path.clone())),
            conf,
        };
        self.source(builder)
    }

    /// Creates a bounded data Stream using a Collection
    ///
    /// Returns a [`Stream`] object that users may execute transformations on.
    ///
    /// Example
    /// ```no_run
    /// use arcon::prelude::*;
    /// let stream: Stream<u64> = Pipeline::default()
    ///     .collection((0..100).collect::<Vec<u64>>(), |conf| {
    ///         conf.set_arcon_time(ArconTime::Process);
    ///     });
    /// ```
    pub fn collection<I, A>(self, i: I, f: impl FnOnce(&mut SourceConf<A>)) -> Stream<A>
    where
        A: ArconType,
        I: Into<Vec<A>> + Send + Sync,
    {
        let collection = i.into();
        let mut conf = SourceConf::default();
        f(&mut conf);

        let builder = SourceBuilder {
            constructor: Arc::new(move |_| collection.clone().into_iter()), // TODO: avoid clone?
            conf,
        };
        self.source(builder)
    }

    /// Creates an unbounded stream using Kafka
    ///
    /// Returns a [`Stream`] object that users may execute transformations on.
    ///
    /// Example
    /// ```no_run
    /// use arcon::prelude::*;
    /// let consumer_conf = KafkaConsumerConf::default()
    ///  .with_topic("test")
    ///  .set("group.id", "test")
    ///  .set("bootstrap.servers", "127.0.0.1:9092")
    ///  .set("enable.auto.commit", "false");
    ///
    /// let stream: Stream<u64> = Pipeline::default()
    ///  .kafka(consumer_conf, JsonSchema::new(), 1, |conf| {
    ///     conf.set_arcon_time(ArconTime::Event);
    ///     conf.set_timestamp_extractor(|x: &u64| *x);
    ///  });
    /// ```
    #[cfg(feature = "kafka")]
    pub fn kafka<S: SourceSchema>(
        self,
        kafka_conf: KafkaConsumerConf,
        schema: S,
        parallelism: usize,
        f: impl FnOnce(&mut SourceConf<S::Data>),
    ) -> Stream<S::Data> {
        let mut conf = SourceConf::default();
        f(&mut conf);

        let builder = ParallelSourceBuilder {
            constructor: Arc::new(move |backend, index, total_sources| {
                KafkaConsumer::new(
                    kafka_conf.clone(),
                    KafkaConsumerState::new(backend),
                    schema.clone(),
                    index,
                    total_sources,
                )
            }),
            conf,
            parallelism,
        };
        self.parallel_source(builder)
    }

    /// Enable DebugNode for the Pipeline
    ///
    ///
    /// The component can be accessed through [method](AssembledPipeline::get_debug_node).
    pub fn with_debug_node(mut self) -> Self {
        self.debug_node_flag = true;
        self
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
    pub fn debug_node_enabled(&self) -> bool {
        self.debug_node_flag
    }

    // internal helper to create a DebugNode from a Stream object
    pub(crate) fn create_debug_node<A>(&mut self, node: DebugNode<A>)
    where
        A: ArconType,
    {
        assert_ne!(
            self.debug_node.is_some(),
            true,
            "DebugNode has already been created!"
        );
        let component = self.ctrl_system.create(|| node);

        self.ctrl_system
            .start_notify(&component)
            .wait_timeout(std::time::Duration::from_millis(500))
            .expect("DebugNode comp never started!");

        self.debug_node = Some(component.clone());
        // define abstract version of the component as the building phase needs it to downcast properly..
        let comp: Arc<dyn AbstractComponent<Message = ArconMessage<A>>> = component;
        self.abstract_debug_node = Some(Arc::new(comp) as ErasedComponent);
    }

    // internal helper to help fetch DebugNode from an AssembledPipeline
    pub(crate) fn get_debug_node<A: ArconType>(&self) -> Option<Arc<Component<DebugNode<A>>>> {
        self.debug_node.as_ref().map(|erased_comp| {
            erased_comp
                .clone()
                .downcast::<Component<DebugNode<A>>>()
                .unwrap()
        })
    }
}
