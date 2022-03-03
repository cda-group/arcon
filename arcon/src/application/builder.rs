use super::conf::ApplicationConf;
use super::Application;
use crate::dataflow::{
    dfg::{DFGNodeKind, GlobalNodeId},
    stream::Context,
};

/// A Builder for Arcon Applications
///
/// ApplicationBuilder may be created through any type that implement the extension
/// trait [ToBuilderExt](../../dataflow/sink/trait.ToBuilderExt.html).
///
/// ## Usage
/// ```no_run
/// use arcon::prelude::*;
/// let mut builder: ApplicationBuilder = (0..100u64)
///     .to_stream(|conf| conf.set_arcon_time(ArconTime::Process))
///     .print()
///     .builder();
///
/// let app: Application = builder.build();
/// ```
#[derive(Default)]
pub struct ApplicationBuilder {
    ctx: Context,
    name: Option<String>,
    debug: bool,
    conf: ApplicationConf,
}

impl ApplicationBuilder {
    pub(crate) fn new(ctx: Context, debug: bool) -> ApplicationBuilder {
        ApplicationBuilder {
            ctx,
            name: None,
            debug,
            conf: ApplicationConf::default(),
        }
    }
    /// Sets the name of the application
    pub fn name(&mut self, name: impl Into<String>) -> &mut Self {
        self.name = Some(name.into());
        self
    }

    /// Sets the configuration that is used during the build phase
    pub fn config(&mut self, conf: ApplicationConf) -> &mut Self {
        self.conf = conf;
        self
    }

    /// Build an Arcon application
    ///
    /// Note that this method only builds the application. In order
    /// to start it, see the following [method](Application::run).
    pub fn build(&mut self) -> Application {
        let mut app = Application::with_conf(self.conf.clone());
        if self.debug {
            app.with_debug_node();
        }

        let mut output_channels = Vec::new();

        for dfg_node in self.ctx.dfg.graph.iter().rev() {
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
                    let sources =
                        source_factory.build_source(output_channels.clone(), Vec::new(), &mut app);
                    app.set_source_manager(sources);
                }
                DFGNodeKind::Node(constructor) => {
                    let components = constructor.build_nodes(
                        node_ids,
                        input_channels.to_vec(),
                        output_channels.clone(),
                        Vec::new(),
                        &mut app,
                    );
                    output_channels = components.iter().map(|(_, c)| c.clone()).collect();
                }
                DFGNodeKind::Placeholder => {
                    panic!("Critical Error, Stream built incorrectly");
                }
            }
        }
        app
    }
}
