#[cfg(all(feature = "metrics", not(feature = "prometheus_exporter")))]
use crate::metrics::log_recorder::LogRecorder;
#[cfg(feature = "kafka")]
use crate::stream::source::{
    kafka::{KafkaConsumer, KafkaConsumerConf, KafkaConsumerState},
    schema::SourceSchema,
};
use crate::{
    application::conf::{logger::ArconLogger, ApplicationConf, ControlPlaneMode},
    buffer::event::PoolInfo,
    control_plane::{
        conf::ControlPlaneConf, distributed::Layout, distributed::ProcessId, ControlPlane,
        ControlPlaneContainer,
    },
    dataflow::{
        api::{ParallelSourceBuilder, SourceBuilder, SourceBuilderType},
        conf::SourceConf,
        constructor::SourceManagerConstructor,
        dfg::*,
        stream::Context,
    },
    prelude::*,
    stream::{
        node::source::SourceEvent,
        source::{local_file::LocalFileSource, Source},
    },
};
use arcon_allocator::Allocator;
use kompact::component::AbstractComponent;
use std::sync::{Arc, Mutex};

pub(crate) mod assembled;
pub mod conf;

pub use crate::dataflow::stream::Stream;
pub use assembled::AssembledApplication;

#[cfg(all(feature = "prometheus_exporter", feature = "metrics", not(test)))]
use metrics_exporter_prometheus::PrometheusBuilder;

/// A Pipeline is the starting point of all Arcon applications.
/// An Application is the starting point of all Arcon applications.
/// It contains all necessary runtime components, configuration,
/// and a custom allocator.
///
/// # Creating a Application
///
/// See [Configuration](ApplicationConf)
///
/// With the default configuration
/// ```no_run
/// use arcon::prelude::Application;
///
/// let app = Application::default();
/// ```
///
/// With configuration
/// ```no_run
/// use arcon::prelude::{Application, ApplicationConf};
///
/// let conf = ApplicationConf {
///     watermark_interval: 2000,
///     ..Default::default()
/// };
/// let app = Application::with_conf(conf);
/// ```
#[derive(Clone)]
pub struct Application {
    /// Configuration for this application
    pub(crate) conf: ApplicationConf,
    /// The DFG of this application
    pub(crate) dfg: DFG,
    /// Arcon allocator for this application
    pub(crate) allocator: Arc<Mutex<Allocator>>,
    /// SourceManager component for this application
    pub(crate) source_manager: Option<Arc<dyn AbstractComponent<Message = SourceEvent>>>,
    /// A container holding information about the application's control plane
    pub(crate) control_plane: ControlPlaneContainer,
    /// Flag indicating whether to spawn a debug node for the Application
    debug_node_flag: bool,
    /// Configured Logger for the Application
    pub(crate) arcon_logger: ArconLogger,
    /// Path to the ApplicationController (only relevant to the workers in distributed applications)
    pub(crate) application_controller: Option<ActorPath>,
    /// The Layout of the application (only relevant to the ApplicationController in distributed applications)
    pub(crate) layout: Layout,
    /// The ProcessId of the local process running this application (only relevant to distributed applications)
    pub(crate) process_id: ProcessId,
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

        let control_plane = match &conf.control_plane_mode {
            ControlPlaneMode::Embedded => {
                let mut cp_conf = ControlPlaneConf::default();
                let mut dir = conf.base_dir.clone();
                dir.push("control_plane");
                cp_conf.dir = dir;
                ControlPlaneContainer::Embedded(ControlPlane::new(cp_conf))
            }
            ControlPlaneMode::Remote(addr) => ControlPlaneContainer::Remote(addr.clone()),
        };

        Self {
            conf,
            dfg: DFG::default(),
            allocator,
            source_manager: None,
            control_plane,
            debug_node_flag: false,
            arcon_logger,
            application_controller: None,
            layout: Layout::new(),
            process_id: 0,
        }
    }

    /// Creates a new Application using the given ApplicationConf
    pub fn with_conf(conf: ApplicationConf) -> Self {
        Self::new(conf)
    }

    pub fn set_layout(&mut self, layout: Layout) {
        self.layout = layout;
    }

    pub fn set_application_controller(&mut self, path: ActorPath) {
        self.application_controller = Some(path);
    }

    pub fn get_application_controller(&self) -> Option<ActorPath> {
        self.application_controller.clone()
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
        let backend = Arc::new(B::create(&state_dir, String::from("source_manager")).unwrap());
        let time = builder_type.time();
        let manager_constructor = SourceManagerConstructor::new(
            String::from("source_manager"),
            builder_type,
            backend,
            self.arcon_conf().watermark_interval,
            time,
        );
        let mut ctx = Context::new(self);
        let kind = DFGNodeKind::Source(Default::default(), Arc::new(manager_constructor));
        let incoming_channels = 0; // sources have 0 incoming channels..
        let outgoing_channels = parallelism;
        let dfg_node = DFGNode::new(kind, outgoing_channels, incoming_channels, vec![]);
        ctx.app.dfg.insert(dfg_node);
        Stream::new(ctx)
    }

    /// Creates a bounded data Stream using a local file
    ///
    /// Returns a [`Stream`] object that users may execute transformations on.
    ///
    /// Example
    /// ```no_run
    /// use arcon::prelude::*;
    /// let stream: Stream<u64> = Application::default()
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
    /// let stream: Stream<u64> = Application::default()
    ///     .iterator(0u64..100, |conf| {
    ///         conf.set_arcon_time(ArconTime::Process);
    ///     });
    /// ```
    pub fn iterator<I>(self, i: I, f: impl FnOnce(&mut SourceConf<I::Item>)) -> Stream<I::Item>
    where
        I: IntoIterator + 'static + Clone + Send + Sync,
        I::IntoIter: Send,
        I::Item: ArconType,
    {
        let mut conf = SourceConf::default();
        f(&mut conf);

        let builder = SourceBuilder {
            constructor: Arc::new(move |_| i.clone().into_iter()),
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
    /// let stream: Stream<u64> = Application::default()
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

    /// Enable DebugNode for the Application
    ///
    ///
    /// The component can be accessed through [method](AssembledApplication::get_debug_node).
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

    /// Give out a reference to the ApplicationConf of the application
    pub(crate) fn arcon_conf(&self) -> &ApplicationConf {
        &self.conf
    }

    pub fn debug_node_enabled(&self) -> bool {
        self.debug_node_flag
    }
}
