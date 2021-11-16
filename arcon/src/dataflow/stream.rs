use crate::{
    application::{assembled::Runtime, Application},
    data::ArconType,
    dataflow::{
        api::OperatorBuilder,
        conf::{DefaultBackend, OperatorConf, ParallelismStrategy},
        constructor::*,
        dfg::{DFGNode, DFGNodeKind, GlobalNodeId, OperatorId},
    },
    index::EmptyState,
    prelude::AssembledApplication,
    stream::operator::{
        function::{Filter, FlatMap, Map, MapInPlace},
        sink::measure::MeasureSink,
        Operator,
    },
    util::ArconFnBounds,
};
use std::{hash::Hash, hash::Hasher, marker::PhantomData, sync::Arc};

use super::dfg::ChannelKind;

#[derive(Default)]
pub struct Context {
    pub(crate) app: Application,
    console_output: bool,
}

impl Context {
    pub fn new(app: Application) -> Self {
        Self {
            app,
            console_output: false,
        }
    }
}

#[derive(Clone)]
pub struct KeyBuilder<T> {
    pub extractor: Arc<(dyn Fn(&T) -> u64 + Send + Sync)>,
}

impl<T: ArconType> KeyBuilder<T> {
    pub fn get_key(&self, event: &T) -> u64 {
        (self.extractor)(event)
    }
}

/// High-level object representing a sequence of stream transformations.
pub struct Stream<T: ArconType> {
    _marker: PhantomData<T>,
    // ID of the node which outputs this stream.
    prev_dfg_id: OperatorId,
    ctx: Context,
    key_builder: Option<KeyBuilder<T>>,
    last_node: Option<Arc<dyn TypedNodeFactory<T>>>,
    source: Option<Arc<dyn TypedSourceFactory<T>>>,
}

impl<T: ArconType> Stream<T> {
    /// Move the optional last_node/source-Factory into the DFG struct, will no longer be mutable after this.
    fn move_last_node(&mut self) {
        if let Some(node) = self.last_node.take() {
            let prev_dfg_node = self.ctx.app.dfg.get_mut(&self.prev_dfg_id);
            assert!(matches!(&prev_dfg_node.kind, DFGNodeKind::Placeholder)); // Make sure nothing bad has happened
            prev_dfg_node.kind = DFGNodeKind::Node(node.untype());
        } else if let Some(source) = self.source.take() {
            let prev_dfg_node = self.ctx.app.dfg.get_mut(&self.prev_dfg_id);
            assert!(matches!(&prev_dfg_node.kind, DFGNodeKind::Placeholder)); // Make sure nothing bad has happened
            prev_dfg_node.kind = DFGNodeKind::Source(source.untype());
        }
    }

    /// Consistently partition the Stream using the given [key_extractor] method.
    /// Also sets the [ChannelKind] of the last node to [ChannelKind::Keyed]
    ///
    /// The [key_extractor] function must be deterministic, for two identical events it
    /// must return the same key whenever it is called.
    ///
    /// Example
    /// ```no_run
    /// use arcon::prelude::*;
    /// let stream: Stream<u64> = Application::default()
    ///     .iterator(0u64..100, |conf| {
    ///         conf.set_arcon_time(ArconTime::Process);
    ///     })
    ///     .key_by(|i: &u64| i);
    pub fn key_by<F, KEY>(mut self, key_extractor: F) -> Stream<T>
    where
        KEY: Hash + 'static,
        F: Fn(&T) -> &KEY + ArconFnBounds,
    {
        let key_builder = KeyBuilder {
            extractor: Arc::new(move |d: &T| {
                let mut hasher = arcon_util::key_hasher();
                key_extractor(d).hash(&mut hasher);
                hasher.finish()
            }),
        };
        if let Some(ref mut node_factory) = self.last_node {
            let mut_node_factory = Arc::get_mut(node_factory).unwrap();
            mut_node_factory.set_key_builder(key_builder.clone());
            self.key_builder = Some(key_builder);
        } else if let Some(ref mut source_factory) = self.source {
            let mut_source_factory = Arc::get_mut(source_factory).unwrap();
            mut_source_factory.set_key_builder(key_builder.clone());
            self.key_builder = Some(key_builder);
        } else {
            panic!("Nothing to apply key_by on!");
        }
        self
    }

    /// Sets the [ChannelKind] of the last operator. The default [ChannelKind] is [ChannelKind::Forward].
    ///
    /// Example
    /// ```no_run
    /// use arcon::prelude::*;
    /// let stream: Stream<u64> = Application::default()
    ///     .iterator(0u64..100, |conf| {
    ///         conf.set_arcon_time(ArconTime::Process);
    ///     })
    ///     .channel_kind(ChannelKind::Forward);
    pub fn channel_kind(mut self, channel_kind: ChannelKind) -> Stream<T> {
        if let Some(ref mut node_factory) = self.last_node {
            let mut_node_factory = Arc::get_mut(node_factory).unwrap();
            mut_node_factory.set_channel_kind(channel_kind);
        } else if let Some(ref mut source_factory) = self.source {
            let mut_source_factory = Arc::get_mut(source_factory).unwrap();
            mut_source_factory.set_channel_kind(channel_kind);
        } else {
            panic!("Nothing to configure ChannelKind on!");
        }
        self
    }

    /// Add an [`Operator`] to the dataflow graph
    ///
    /// Example
    /// ```no_run
    /// use arcon::prelude::*;
    /// let stream: Stream<u64> = Application::default()
    ///     .iterator(0u64..100, |conf| {
    ///         conf.set_arcon_time(ArconTime::Process);
    ///     })
    ///     .operator(OperatorBuilder {
    ///         operator: Arc::new(|| Map::new(|x| x + 10)),
    ///         state: Arc::new(|_| EmptyState),
    ///         conf: Default::default(),
    ///     });
    /// ```
    pub fn operator<OP>(mut self, builder: OperatorBuilder<OP>) -> Stream<OP::OUT>
    where
        OP: Operator<IN = T> + 'static,
    {
        // No more mutations on the previous node, move it from the stream.current_node to the DFG Graph
        self.move_last_node();

        // Set up directory for the operator and create Backend
        let mut state_dir = self.ctx.app.arcon_conf().state_dir();
        let state_id = builder.state_id();
        state_dir.push(state_id);

        let paralellism = match builder.conf.parallelism_strategy {
            ParallelismStrategy::Static(num) => num,
            _ => unreachable!("Managed Parallelism not Supported yet"),
        };

        let prev_dfg_node = self.ctx.app.dfg.get_mut(&self.prev_dfg_id);
        let incoming_channels = prev_dfg_node.get_node_ids();
        let operator_id = prev_dfg_node.get_operator_id() + 1;

        let node_constructor = NodeConstructor::<OP, DefaultBackend>::new(
            format!("Operator_{}", operator_id),
            state_dir,
            Arc::new(builder),
            self.ctx.app.arcon_logger.clone(),
            self.key_builder.take(),
        );

        let dfg_node = DFGNode::new(
            DFGNodeKind::Placeholder, // The NodeFactory will be inserted into the DFG when it is finalized
            operator_id,
            paralellism,
            incoming_channels,
        );
        prev_dfg_node.set_outgoing_channels(dfg_node.get_node_ids());
        let next_dfg_id = self.ctx.app.dfg.insert(dfg_node);

        self.prev_dfg_id = next_dfg_id;
        Stream {
            _marker: PhantomData,
            prev_dfg_id: self.prev_dfg_id,
            ctx: self.ctx,
            last_node: Some(Arc::new(node_constructor)),
            key_builder: None,
            source: None,
        }
    }

    /// Adds a stateless Map operator with default configuration to the application
    ///
    /// If you need a stateful version or control over the configuration, use the operator function directly!
    ///
    /// Example
    /// ```no_run
    /// use arcon::prelude::*;
    /// let stream: Stream<u64> = Application::default()
    ///     .iterator(0..100, |conf| {
    ///         conf.set_arcon_time(ArconTime::Process);
    ///     })
    ///     .map(|x| x + 10);
    /// ```
    pub fn map<F, OUT>(self, f: F) -> Stream<OUT>
    where
        OUT: ArconType,
        F: Fn(T) -> OUT + ArconFnBounds,
    {
        self.operator(OperatorBuilder {
            operator: Arc::new(move || Map::new(f.clone())),
            state: Arc::new(|_| EmptyState),
            conf: Default::default(),
        })
    }

    /// Adds a stateless MapInPlace operator with default configuration to the application
    ///
    /// If you need a stateful version or control over the configuration, use the operator function directly!
    ///
    /// Example
    /// ```no_run
    /// use arcon::prelude::*;
    /// let stream: Stream<u64> = Application::default()
    ///     .iterator(0..100, |conf| {
    ///         conf.set_arcon_time(ArconTime::Process);
    ///     })
    ///     .map_in_place(|x| *x += 10);
    /// ```
    pub fn map_in_place<F>(self, f: F) -> Stream<T>
    where
        F: Fn(&mut T) + ArconFnBounds,
    {
        self.operator(OperatorBuilder {
            operator: Arc::new(move || MapInPlace::new(f.clone())),
            state: Arc::new(|_| EmptyState),
            conf: Default::default(),
        })
    }

    /// Adds a stateless Filter operator with default configuration to the application
    ///
    /// If you need a stateful version or control over the configuration, use the operator function directly!
    ///
    /// Example
    /// ```no_run
    /// use arcon::prelude::*;
    /// let stream: Stream<u64> = Application::default()
    ///     .iterator(0..100, |conf| {
    ///         conf.set_arcon_time(ArconTime::Process);
    ///     })
    ///     .filter(|x| x < &50);
    /// ```
    pub fn filter<F>(self, f: F) -> Stream<T>
    where
        F: Fn(&T) -> bool + ArconFnBounds,
    {
        self.operator(OperatorBuilder {
            operator: Arc::new(move || Filter::new(f.clone())),
            state: Arc::new(|_| EmptyState),
            conf: Default::default(),
        })
    }

    /// Adds a stateless Flatmap operator with default configuration to the application
    ///
    /// If you need a stateful version or control over the configuration, use the operator function directly!
    ///
    /// Example
    /// ```no_run
    /// use arcon::prelude::*;
    /// let stream: Stream<u64> = Application::default()
    ///     .iterator(0..100, |conf| {
    ///         conf.set_arcon_time(ArconTime::Process);
    ///     })
    ///     .flatmap(|x| (0..x));
    /// ```
    pub fn flatmap<F, OUTS>(self, f: F) -> Stream<OUTS::Item>
    where
        OUTS: IntoIterator + 'static,
        OUTS::Item: ArconType,
        F: Fn(T) -> OUTS + ArconFnBounds,
    {
        self.operator(OperatorBuilder {
            operator: Arc::new(move || FlatMap::new(f.clone())),
            state: Arc::new(|_| EmptyState),
            conf: Default::default(),
        })
    }

    /// Will make sure the most downstream Node will print its result to the console
    ///
    /// Note that if the Application has been configured with a debug node, it will take precedence.
    #[allow(clippy::wrong_self_convention)]
    pub fn to_console(mut self) -> Stream<T> {
        self.ctx.console_output = true;

        Stream {
            _marker: PhantomData,
            prev_dfg_id: self.prev_dfg_id,
            ctx: self.ctx,
            last_node: self.last_node,
            key_builder: self.key_builder,
            source: self.source,
        }
    }

    /// Adds a final MeasureSink before building an AssembledApplication
    ///
    /// `log_frequency` can be used to tune how often (e.g., every 1000000 events) measurements are logged.
    ///
    /// Note: For more stable outputs, use a somewhat large loq frequency > 1000000
    pub fn measure(self, log_frequency: u64) -> AssembledApplication {
        let stream = self.operator(OperatorBuilder {
            operator: Arc::new(move || MeasureSink::new(log_frequency)),
            state: Arc::new(|_| EmptyState),
            conf: OperatorConf {
                parallelism_strategy: ParallelismStrategy::Static(1),
                ..Default::default()
            },
        });
        stream.build()
    }

    /// Builds the Dataflow graph
    ///
    /// Returns a [`AssembledApplication`] where all runtime components
    /// have been conneted and started.
    ///
    /// Note that this method only builds the application. In order
    /// to start it, see the following [method](AssembledApplication::start).
    pub fn build(mut self) -> AssembledApplication {
        self.move_last_node();
        let runtime = self.build_runtime();
        let mut assembled = AssembledApplication::new(self.ctx.app.clone(), runtime);

        let mut output_channels = Vec::new();

        for dfg_node in self.ctx.app.dfg.graph.iter().rev() {
            let operator_id = dfg_node.get_operator_id();
            let input_channels = dfg_node.get_input_channels();
            let node_ids = dfg_node
                .get_node_ids()
                .iter()
                .map(|id| GlobalNodeId {
                    operator_id,
                    node_id: *id,
                })
                .collect();
            match &dfg_node.kind {
                DFGNodeKind::Source(source_factory) => {
                    let sources = source_factory.build_source(
                        output_channels.clone(),
                        Vec::new(),
                        &mut assembled,
                    );
                    assembled.set_source_manager(sources);
                }
                DFGNodeKind::Node(constructor) => {
                    let components = constructor.build_nodes(
                        node_ids,
                        input_channels.to_vec(),
                        output_channels.clone(),
                        Vec::new(),
                        &mut assembled,
                    );
                    output_channels = components.iter().map(|(_, c)| c.clone()).collect();
                }
                DFGNodeKind::Placeholder => {
                    panic!("Critical Error, Stream built incorrectly");
                }
            }
        }
        assembled
    }

    fn build_runtime(&mut self) -> Runtime {
        Runtime::new(self.ctx.app.arcon_conf(), &self.ctx.app.arcon_logger)
    }

    pub(crate) fn new(ctx: Context) -> Self {
        Self {
            _marker: PhantomData,
            prev_dfg_id: 0,
            ctx,
            last_node: None,
            key_builder: None,
            source: None,
        }
    }
}
