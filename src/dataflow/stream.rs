// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconType, NodeID},
    dataflow::{
        conf::{OperatorBuilder, ParallelismStrategy},
        constructor::*,
        dfg::{ChannelKind, DFGNode, DFGNodeID, DFGNodeKind, DFG},
    },
    pipeline::{AssembledPipeline, Pipeline},
    stream::{node::debug::DebugNode, operator::Operator},
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

        let outgoing_channels = match builder.conf.parallelism_strategy {
            ParallelismStrategy::Static(num) => num,
            _ => unreachable!("Managed Parallelism not Supported yet"),
        };

        let manager_constructor = node_manager_constructor::<OP, _>(
            state_id,
            self.ctx.pipeline.data_system.clone(),
            builder,
            backend,
        );

        let prev_dfg_node = self.ctx.dfg.get_mut(&self.prev_dfg_id);
        let incoming_channels = prev_dfg_node.outgoing_channels;

        let next_dfg_id = self.ctx.dfg.insert(DFGNode::new(
            DFGNodeKind::Node(manager_constructor),
            outgoing_channels,
            incoming_channels,
            vec![self.prev_dfg_id],
        ));

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
                DFGNodeKind::Source(channel_kind, source_manager_cons) => {
                    let nodes = target_nodes.take().unwrap();
                    let source_manager =
                        source_manager_cons(nodes, channel_kind, &mut self.ctx.pipeline);

                    self.ctx.pipeline.source_manager = Some(source_manager);
                }
                DFGNodeKind::Node(manager_cons) => {
                    let (channel_kind, components) = {
                        match target_nodes {
                            Some(comps) => (dfg_node.channel_kind, comps),
                            None => {
                                // At the end of the graph....
                                if self.ctx.pipeline.debug_node_enabled() {
                                    let node: DebugNode<IN> = DebugNode::new();
                                    self.ctx.pipeline.create_debug_node(node);
                                }

                                match self.ctx.pipeline.abstract_debug_node {
                                    Some(ref debug_node) => {
                                        (ChannelKind::Forward, vec![debug_node.clone()])
                                    }
                                    None => (
                                        if self.ctx.console_output {
                                            ChannelKind::Console
                                        } else {
                                            ChannelKind::Mute
                                        },
                                        vec![],
                                    ),
                                }
                            }
                        }
                    };

                    // Create expected incoming channels ids
                    let in_channels: Vec<NodeID> = (0..dfg_node.ingoing_channels)
                        .map(|i| NodeID::new(i as u32))
                        .collect();

                    let nodes = manager_cons(
                        in_channels,
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
