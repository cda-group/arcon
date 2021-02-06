// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconType, NodeID},
    dataflow::{
        conf::OperatorBuilder,
        constructor::*,
        dfg::{ChannelKind, DFGNode, DFGNodeID, DFGNodeKind, SourceKind, DFG},
    },
    pipeline::{AssembledPipeline, Pipeline},
    stream::operator::Operator,
};
use std::{marker::PhantomData, sync::Arc};

#[derive(Default)]
pub struct Context {
    pub(crate) dfg: DFG,
    pipeline: Pipeline,
    console_output: bool,
}

impl Context {
    pub fn new(pipeline: Pipeline) -> Self {
        Self {
            dfg: Default::default(),
            pipeline,
            console_output: false,
        }
    }
}

/// High-level object representing a sequence of stream transformations.
pub struct Stream<IN: ArconType> {
    _marker: PhantomData<IN>,
    // ID of the node which outputs this stream.
    prev_dfg_id: DFGNodeID,
    ctx: Context,
}

impl<IN: ArconType> Stream<IN> {
    /// Add an [`Operator`] to the dataflow graph
    pub fn operator<OP>(mut self, builder: OperatorBuilder<OP>) -> Stream<OP::OUT>
    where
        OP: Operator<IN = IN> + 'static,
    {
        // Set up directory for the operator and create Backend
        let mut state_dir = self.ctx.pipeline.arcon_conf().state_dir.clone();
        let state_id = builder.state_id();
        state_dir.push(state_id.clone());
        let backend = builder.create_backend(state_dir);

        let manager_constructor = node_manager_constructor::<OP, _>(
            state_id,
            self.ctx.pipeline.data_system.clone(),
            builder,
            backend,
        );

        let next_dfg_id = self
            .ctx
            .dfg
            .insert(DFGNode::new(DFGNodeKind::Node(manager_constructor), vec![
                self.prev_dfg_id,
            ]));

        self.prev_dfg_id = next_dfg_id;
        Stream {
            _marker: PhantomData,
            prev_dfg_id: self.prev_dfg_id,
            ctx: self.ctx,
        }
    }

    /// Will make sure the most downstream Node will print its result to the console
    #[allow(clippy::wrong_self_convention)]
    pub fn to_console(mut self) -> Stream<IN> {
        self.ctx.console_output = true;

        Stream {
            _marker: PhantomData,
            prev_dfg_id: self.prev_dfg_id,
            ctx: self.ctx,
        }
    }

    /// Builds the Dataflow graph
    ///
    /// Returns a [`AssembledPipeline`] where all runtime components
    /// have been conneted and started.
    ///
    /// Note that this method only builds the pipeline. In order
    /// to start it, see the following [method](AssembledPipeline::start).
    pub fn build(mut self) -> AssembledPipeline {
        let mut target_nodes: Option<Vec<Arc<dyn std::any::Any + Send + Sync>>> = None;

        for dfg_node in self.ctx.dfg.graph.into_iter().rev() {
            match dfg_node.kind {
                DFGNodeKind::Source(source_kind, channel_kind, source_manager_cons) => {
                    match source_kind {
                        SourceKind::Single(constructor) => {
                            let comp = constructor(
                                target_nodes.take().unwrap(),
                                channel_kind,
                                &mut self.ctx.pipeline.data_system(),
                            );

                            let source_manager =
                                source_manager_cons(vec![comp], &mut self.ctx.pipeline);

                            self.ctx.pipeline.source_manager = Some(source_manager);
                        }
                        SourceKind::Parallel => {}
                    }
                }
                DFGNodeKind::Node(manager_cons) => {
                    let (channel_kind, components) = {
                        match target_nodes {
                            Some(comps) => (dfg_node.channel_kind, comps),
                            None => (
                                if self.ctx.console_output {
                                    ChannelKind::Console
                                } else {
                                    ChannelKind::Mute
                                },
                                vec![],
                            ),
                        }
                    };

                    let nodes = manager_cons(
                        vec![NodeID::new(0)],
                        components,
                        channel_kind,
                        &mut self.ctx.pipeline,
                    );

                    target_nodes = Some(nodes);
                }
            }
        }
        AssembledPipeline::new(self.ctx.pipeline)
    }

    pub(crate) fn new(ctx: Context) -> Self {
        Self {
            _marker: PhantomData,
            prev_dfg_id: DFGNodeID(0),
            ctx,
        }
    }
}
