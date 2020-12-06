// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    buffer::event::PoolInfo,
    conf::{ArconConf, ExecutionMode},
    dataflow::stream::Context,
    manager::{
        epoch::EpochManager,
        node::*,
        source::SourceManager,
        state::{SnapshotRef, StateManager, StateManagerPort},
    },
    prelude::*,
    stream::source::ArconSource,
};
use arcon_allocator::Allocator;
use kompact::{component::AbstractComponent, prelude::KompactSystem};
use std::sync::{Arc, Mutex};

pub use crate::dataflow::stream::Stream;

#[derive(Clone)]
pub struct Pipeline {
    /// [`KompactSystem`] for Control Components
    ctrl_system: KompactSystem,
    /// [`KompactSystem`] for Data Processing Components
    data_system: KompactSystem,
    /// Arcon configuration for this pipeline
    conf: ArconConf,
    /// Arcon allocator for this pipeline
    allocator: Arc<Mutex<Allocator>>,
    /// SourceManager component for this pipeline
    source_manager: Arc<Component<SourceManager>>,
    /// EpochManager component for this pipeline
    epoch_manager: Option<Arc<Component<EpochManager>>>,
    /// StateManager component for this pipeline
    state_manager: Arc<Component<StateManager>>,
}

impl Default for Pipeline {
    fn default() -> Self {
        let conf: ArconConf = Default::default();
        Self::new(conf)
    }
}

/// A Node with operator type, state backend type, and timer type erased
pub type DynamicNode<IN> = Box<dyn CreateErased<ArconMessage<IN>>>;
/// Result of creating a [`DynamicNode`] in a [`KompactSystem`](kompact::prelude::KompactSystem)
pub type CreatedDynamicNode<IN> = Arc<dyn AbstractComponent<Message = ArconMessage<IN>>>;
/// A Source with operator type, state backend type, and timer type erased
pub type DynamicSource = Box<dyn CreateErased<()>>;

impl Pipeline {
    /// Creates a new Pipeline using the given ArconConf
    fn new(conf: ArconConf) -> Self {
        let allocator = Arc::new(Mutex::new(Allocator::new(conf.allocator_capacity)));
        let (ctrl_system, data_system, state_manager, epoch_manager, source_manager) =
            Self::setup(&conf);

        Self {
            ctrl_system,
            data_system,
            conf,
            allocator,
            epoch_manager,
            state_manager,
            source_manager,
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
    ) -> (
        KompactSystem,
        KompactSystem,
        Arc<Component<StateManager>>,
        Option<Arc<Component<EpochManager>>>,
        Arc<Component<SourceManager>>,
    ) {
        let kompact_config = arcon_conf.kompact_conf();
        let data_system = kompact_config.build().expect("KompactSystem");
        // just build a default system for control layer
        let ctrl_system = KompactConfig::default().build().unwrap();

        let state_manager_comp = ctrl_system.create_dedicated(StateManager::new);
        let source_manager_comp = ctrl_system.create(SourceManager::new);

        let epoch_manager = match arcon_conf.execution_mode {
            ExecutionMode::Local => {
                let source_manager_ref = source_manager_comp.actor_ref().hold().expect("fail");
                let epoch_manager =
                    EpochManager::new(arcon_conf.epoch_interval, source_manager_ref);
                Some(ctrl_system.create(|| epoch_manager))
            }
            ExecutionMode::Distributed => None,
        };

        let timeout = std::time::Duration::from_millis(500);

        ctrl_system
            .start_notify(&state_manager_comp)
            .wait_timeout(timeout)
            .expect("StateManager comp never started!");

        ctrl_system
            .start_notify(&source_manager_comp)
            .wait_timeout(timeout)
            .expect("SourceManager comp never started!");

        (
            ctrl_system,
            data_system,
            state_manager_comp,
            epoch_manager,
            source_manager_comp,
        )
    }

    /// Creates a PoolInfo struct to be used by a ChannelStrategy
    pub fn get_pool_info(&self) -> PoolInfo {
        PoolInfo::new(
            self.conf.channel_batch_size,
            self.conf.buffer_pool_size,
            self.conf.buffer_pool_limit,
            self.allocator.clone(),
        )
    }

    pub fn connect_state_port<B: Backend>(&mut self, nm: &Arc<Component<NodeManager<B>>>) {
        biconnect_components::<StateManagerPort, _, _>(&self.state_manager, nm)
            .expect("connection");
    }

    /// Add component `c` to receive state snapshots from `state_id`
    pub fn watch(
        &mut self,
        state_ids: Vec<impl Into<StateID>>,
        c: Arc<dyn AbstractComponent<Message = SnapshotRef>>,
    ) {
        self.state_manager.on_definition(|cd| {
            for id in state_ids.into_iter() {
                let state_id = id.into();

                if !cd.registered_state_ids.contains(&state_id) {
                    panic!(
                        "State id {} has not been registered at the StateManager",
                        state_id
                    );
                }

                let actor_ref = c.actor_ref().hold().expect("fail");

                cd.subscribers
                    .entry(state_id)
                    .or_insert_with(Vec::new)
                    .push(actor_ref);
            }
        });
    }

    /// Give out a mutable reference to the KompactSystem of the pipeline
    pub fn system(&mut self) -> &mut KompactSystem {
        &mut self.data_system
    }

    /// Give out a reference to the ArconConf of the pipeline
    pub fn arcon_conf(&self) -> &ArconConf {
        &self.conf
    }

    /// Adds a NodeManager to the Arcon Pipeline
    pub fn create_node_manager(
        &mut self,
        _id: String,
        _in_channels: Vec<NodeID>,
        _backend: impl Backend,
    ) {
        unimplemented!();
    }

    /// Creates a bounded data source using a Vector of [`ArconType`]
    pub fn collection<I, A>(self, i: I) -> Stream<A>
    where
        I: Into<Vec<A>>,
        A: ArconType,
    {
        // TODO: Add DFGNode with Collection type..
        // DFGNode::Source(...)
        let ctx = Context::new(self);
        Stream::new(ctx)
    }

    /// Instructs the SourceManager of the pipeline
    /// to inject a start message to the source components
    /// of the pipeline.
    ///
    /// The function will panic if no sources have been created
    pub fn start(&self) {
        let sources = self.source_manager.on_definition(|cd| cd.sources.len());
        assert_ne!(
            sources, 0,
            "No source components have been created, cannot start the pipeline!"
        );
        self.source_manager.actor_ref().tell(ArconSource::Start);
    }

    /// Awaits termination from the pipeline
    ///
    /// Note that this blocks the current thread
    pub fn await_termination(self) {
        self.data_system.await_termination();
        self.ctrl_system.await_termination();
    }

    /// Shuts the pipeline down and consumes the struct
    pub fn shutdown(self) {
        let _ = self.data_system.shutdown();
        let _ = self.ctrl_system.shutdown();
    }
}
