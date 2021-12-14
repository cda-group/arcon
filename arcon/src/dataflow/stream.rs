use crate::{
    application::{assembled::Runtime, Application},
    data::ArconType,
    dataflow::{
        api::OperatorBuilder,
        conf::{OperatorConf, ParallelismStrategy},
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

use std::{marker::PhantomData, sync::Arc};

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

/// High-level object representing a sequence of stream transformations.
pub struct Stream<IN: ArconType> {
    _marker: PhantomData<IN>,
    // ID of the node which outputs this stream.
    prev_dfg_id: OperatorId,
    ctx: Context,
}

impl<IN: ArconType> Stream<IN> {
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
        F: Fn(IN) -> OUT + ArconFnBounds,
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
    pub fn map_in_place<F>(self, f: F) -> Stream<IN>
    where
        F: Fn(&mut IN) + ArconFnBounds,
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
    pub fn filter<F>(self, f: F) -> Stream<IN>
    where
        F: Fn(&IN) -> bool + ArconFnBounds,
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
        F: Fn(IN) -> OUTS + ArconFnBounds,
    {
        self.operator(OperatorBuilder {
            operator: Arc::new(move || FlatMap::new(f.clone())),
            state: Arc::new(|_| EmptyState),
            conf: Default::default(),
        })
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
        OP: Operator<IN = IN> + 'static,
    {
        // Set up directory for the operator and create Backend
        let mut state_dir = self.ctx.app.arcon_conf().state_dir();
        let state_id = builder.state_id();
        state_dir.push(state_id);

        let paralellism = match builder.conf.parallelism_strategy {
            ParallelismStrategy::Static(num) => num,
            _ => unreachable!("Managed Parallelism not Supported yet"),
        };

        let manager_constructor = NodeConstructor::new(
            "Operator".to_string(),
            state_dir,
            Arc::new(builder),
            self.ctx.app.arcon_logger.clone(),
        );

        let prev_dfg_node = self.ctx.app.dfg.get_mut(&self.prev_dfg_id);
        let incoming_channels = prev_dfg_node.get_node_ids();
        let operator_id = prev_dfg_node.get_operator_id() + 1;
        let dfg_node = DFGNode::new(
            DFGNodeKind::Node(Arc::new(manager_constructor)),
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
        }
    }

    /// Will make sure the most downstream Node will print its result to the console
    ///
    /// Note that if the Application has been configured with a debug node, it will take precedence.
    #[allow(clippy::wrong_self_convention)]
    pub fn to_console(mut self) -> Stream<IN> {
        self.ctx.console_output = true;

        Stream {
            _marker: PhantomData,
            prev_dfg_id: self.prev_dfg_id,
            ctx: self.ctx,
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
                    eprintln!("Building a Source");
                    let sources = source_factory.build_source(
                        output_channels.clone(),
                        Vec::new(),
                        &mut assembled,
                    );
                    assembled.set_source_manager(sources);
                    eprintln!("Source Built");
                }
                DFGNodeKind::Node(constructor) => {
                    eprintln!("Building a Node");
                    let components = constructor.build_nodes(
                        node_ids,
                        input_channels.to_vec(),
                        output_channels.clone(),
                        Vec::new(),
                        &mut assembled,
                    );
                    output_channels = components.iter().map(|(_, c)| c.clone()).collect();
                    eprintln!("Node Built");
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
        }
    }
}
