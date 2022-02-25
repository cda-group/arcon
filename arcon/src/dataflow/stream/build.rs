use crate::{
    application::assembled::AssembledApplication,
    data::ArconType,
    dataflow::{
        dfg::{DFGNodeKind, GlobalNodeId},
        stream::Stream,
    },
};

use super::keyed::KeyedStream;

/// Extension trait for build operations
pub trait BuildExt {
    /// Builds the Dataflow graph
    ///
    /// Returns an [`AssembledApplication`] where all runtime components
    /// have been conneted and started.
    ///
    /// Note that this method only builds the application. In order
    /// to start it, see the following [method](AssembledApplication::start).
    fn build(self) -> AssembledApplication;
}

impl<T: ArconType> BuildExt for Stream<T> {
    fn build(mut self) -> AssembledApplication {
        self.move_last_node();
        let runtime = self.build_runtime();
        let app = self.ctx.app;
        let mut assembled = AssembledApplication::new(app.clone(), runtime);

        let mut output_channels = Vec::new();

        for dfg_node in app.dfg.graph.iter().rev() {
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
}

impl<T: ArconType> BuildExt for KeyedStream<T> {
    fn build(self) -> AssembledApplication {
        self.stream.build()
    }
}
