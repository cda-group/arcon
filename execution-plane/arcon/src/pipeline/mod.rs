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
use kompact::prelude::{Component, KompactSystem};
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
    fn setup(arcon_conf: &ArconConf) -> (KompactSystem, Arc<Component<StateManager>>) {
        let kompact_config = arcon_conf.kompact_conf();
        let system = kompact_config.build().expect("KompactSystem");
        let state_manager = system.create(|| StateManager::new(ExecutionMode::Single));

        let timeout = std::time::Duration::from_millis(500);
        system
            .start_notify(&state_manager)
            .wait_timeout(timeout)
            .expect("state_manager never started!");

        (system, state_manager)
    }

    /// Give out a reference to the KompactSystem of the pipeline
    ///
    /// Useful for testing purposes
    pub(crate) fn system(&mut self) -> &mut KompactSystem {
        &mut self.system
    }

    /// Adds a NodeManager to the Arcon Pipeline
    pub fn create_node_manager<IN, OUT>(
        &mut self,
        node_description: String,
        node_fn: &'static dyn SafelySendableFn(
            NodeID,
            Vec<NodeID>,
            ChannelStrategy<OUT>,
        ) -> Node<IN, OUT>,
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
            node_description,
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

    /// Awaits termination from the pipeline
    pub fn await_termination(self) {
        // NOTE: Blocking call
        self.system.await_termination();
    }

    /// Shuts the pipeline down and consumes the struct
    pub fn shutdown(self) {
        let _ = self.system.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_pipeline_test() {
        let mut pipeline = ArconPipeline::new();
        let ref mut system = pipeline.system();
        // Create a Debug Node for test purposes
        let sink = system.create(move || DebugNode::<u32>::new());
        system.start(&sink);
        let actor_ref: ActorRefStrong<ArconMessage<u32>> =
            sink.actor_ref().hold().expect("Failed to fetch");
        let channel = Channel::Local(actor_ref);
        let channel_strategy: ChannelStrategy<u32> =
            ChannelStrategy::Forward(Forward::new(channel, NodeID::new(0)));

        // Define the function to create our Node
        fn node_fn(
            id: NodeID,
            in_channels: Vec<NodeID>,
            channel_strategy: ChannelStrategy<u32>,
        ) -> Node<u32, u32> {
            #[inline]
            fn map_fn(u: u32) -> u32 {
                u
            }

            Node::new(
                id,
                in_channels,
                channel_strategy,
                Box::new(Map::new(&map_fn)),
                Box::new(InMemory::new("perf").unwrap()),
            )
        }

        let node_one = node_fn(
            NodeID::new(0),
            vec![NodeID::new(1)],
            channel_strategy.clone(),
        );

        // Create node manager
        pipeline.create_node_manager(
            String::from("simple_map_node"),
            &node_fn,
            vec![NodeID::new(1)],
            channel_strategy,
            vec![node_one],
        );

        pipeline.shutdown();
    }
}
