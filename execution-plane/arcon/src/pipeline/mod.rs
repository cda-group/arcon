// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    conf::ArconConf,
    manager::{
        node_manager::*,
        state_manager::{ExecutionMode, StateManager, StateManagerPort},
    },
    prelude::*,
    util::SafelySendableFn,
};
use kompact::prelude::{Component, KompactConfig, KompactSystem};
use std::sync::Arc;

/// A struct meant to simplify the creation of an Arcon Pipeline
#[derive(Clone)]
pub struct ArconPipeline {
    /// [kompact] system that drives the execution of components
    system: KompactSystem,
    /// Arcon configuration for this pipeline
    conf: ArconConf,
    /// The state manager for this pipeline
    state_manager: Arc<Component<StateManager>>,
    /// Amount of NodeManagers launched on top of this ArconPipeline
    node_managers: Vec<ActorRefStrong<NodeEvent>>,
}

impl ArconPipeline {
    /// Creates a new ArconPipeline using the default ArconConf
    pub fn new() -> ArconPipeline {
        let conf = ArconConf::default();
        let (system, state_manager) = ArconPipeline::setup(&conf);

        ArconPipeline {
            system,
            conf,
            state_manager,
            node_managers: Vec::new(),
        }
    }

    /// Creates a new ArconPipeline using the given ArconConf
    pub fn with_conf(conf: ArconConf) -> ArconPipeline {
        let (system, state_manager) = ArconPipeline::setup(&conf);

        ArconPipeline {
            system,
            conf,
            state_manager,
            node_managers: Vec::new(),
        }
    }
    /// Helper function to set up internals of the pipeline
    fn setup(_: &ArconConf) -> (KompactSystem, Arc<Component<StateManager>>) {
        let system = KompactConfig::default().build().expect("KompactSystem");
        let state_manager = system.create(|| StateManager::new(ExecutionMode::Single));

        let timeout = std::time::Duration::from_millis(500);
        system
            .start_notify(&state_manager)
            .wait_timeout(timeout)
            .expect("state_manager never started!");

        (system, state_manager)
    }

    /// Adds a NodeManager to the Arcon Pipeline
    pub fn create_node_manager<IN, OUT>(
        &mut self,
        node_fn: &'static dyn SafelySendableFn(
            NodeID,
            Vec<NodeID>,
            ChannelStrategy<OUT>,
        ) -> Arc<Component<Node<IN, OUT>>>,
        in_channels: Vec<NodeID>,
        channel_strategy: ChannelStrategy<OUT>,
        nodes: Vec<Node<IN, OUT>>,
    ) where
        IN: ArconType,
        OUT: ArconType,
    {
        let timeout = std::time::Duration::from_millis(500);
        let mut node_comps = Vec::with_capacity(nodes.len());
        // Create Node components
        for node in nodes {
            let node_comp = self.system.create(|| node);
            // Connect each node with a port to the state manager
            biconnect_components::<StateManagerPort, _, _>(&self.state_manager, &node_comp)
                .expect("failed to connect port");
            self.system
                .start_notify(&node_comp)
                .wait_timeout(timeout)
                .expect("node never started!");
            node_comps.push(node_comp);
        }

        let node_manager = NodeManager::new(
            node_fn,
            channel_strategy,
            in_channels,
            node_comps,
            self.state_manager.clone(),
            None,
            None,
        );

        let node_manager_comp = self.system.create(|| node_manager);
        let node_manager_ref = node_manager_comp.actor_ref().hold().expect("no");
        self.node_managers.push(node_manager_ref);

        self.system
            .start_notify(&node_manager_comp)
            .wait_timeout(timeout)
            .expect("node_manager never started!");
    }

    /// Awaits termination from the pipelines KompactSystem
    pub fn await_termination(self) {
        // Blocking call
        self.system.await_termination();
    }
}
