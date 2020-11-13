// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

#[cfg(feature = "arcon_tui")]
use arcon_tui::{component::TuiComponent, widgets::node::Node as TuiNode};

use crate::{
    buffer::event::PoolInfo,
    conf::{ArconConf, ExecutionMode},
    manager::{
        epoch::EpochManager,
        node::*,
        source::SourceManager,
        state::{SnapshotRef, StateManager, StateManagerPort},
    },
    prelude::*,
};
use arcon_allocator::Allocator;
use kompact::{component::AbstractComponent, prelude::KompactSystem};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Pipeline {
    /// [kompact] system that drives the execution of components
    system: KompactSystem,
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
    #[cfg(feature = "arcon_tui")]
    tui_component: Arc<Component<TuiComponent>>,
    #[cfg(feature = "arcon_tui")]
    arcon_event_receiver: Arc<arcon_tui::Receiver<TuiNode>>,
}

/// A Node with operator type, state backend type, and timer type erased
pub type DynamicNode<IN> = Box<dyn CreateErased<ArconMessage<IN>>>;
/// Result of creating a [`DynamicNode`] in a [`KompactSystem`](kompact::KompactSystem)
pub type CreatedDynamicNode<IN> = Arc<dyn AbstractComponent<Message = ArconMessage<IN>>>;
/// A Source with operator type, state backend type, and timer type erased
pub type DynamicSource = Box<dyn CreateErased<()>>;

impl Pipeline {
    /// Creates a new Pipeline using the default ArconConf
    pub fn new() -> Self {
        let conf: ArconConf = Default::default();
        let allocator = Arc::new(Mutex::new(Allocator::new(conf.allocator_capacity)));
        #[cfg(feature = "arcon_tui")]
        let (system, state_manager, epoch_manager, source_manager, tui_component, arcon_receiver) =
            Self::setup(&conf);
        #[cfg(not(feature = "arcon_tui"))]
        let (system, state_manager, epoch_manager, source_manager) = Self::setup(&conf);

        Self {
            system,
            conf,
            allocator,
            epoch_manager,
            state_manager,
            source_manager,
            #[cfg(feature = "arcon_tui")]
            tui_component,
            #[cfg(feature = "arcon_tui")]
            arcon_event_receiver: Arc::new(arcon_receiver),
        }
    }

    /// Creates a new Pipeline using the given ArconConf
    pub fn with_conf(conf: ArconConf) -> Self {
        let allocator = Arc::new(Mutex::new(Allocator::new(conf.allocator_capacity)));
        #[cfg(feature = "arcon_tui")]
        let (system, state_manager, epoch_manager, source_manager, tui_component, arcon_receiver) =
            Self::setup(&conf);
        #[cfg(not(feature = "arcon_tui"))]
        let (system, state_manager, epoch_manager, source_manager) = Self::setup(&conf);

        Self {
            system,
            conf,
            allocator,
            epoch_manager,
            state_manager,
            source_manager,
            #[cfg(feature = "arcon_tui")]
            tui_component,
            #[cfg(feature = "arcon_tui")]
            arcon_event_receiver: Arc::new(arcon_receiver),
        }
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

    /// Helper function to set up internals of the pipeline
    #[cfg(not(feature = "arcon_tui"))]
    fn setup(
        arcon_conf: &ArconConf,
    ) -> (
        KompactSystem,
        Arc<Component<StateManager>>,
        Option<Arc<Component<EpochManager>>>,
        Arc<Component<SourceManager>>,
    ) {
        let kompact_config = arcon_conf.kompact_conf();
        let system = kompact_config.build().expect("KompactSystem");
        let state_manager = StateManager::new();
        let state_manager_comp = system.create_dedicated(|| state_manager);

        let source_manager = SourceManager::new();
        let source_manager_comp = system.create(|| source_manager);

        let epoch_manager = match arcon_conf.execution_mode {
            ExecutionMode::Local => {
                let source_manager_ref = source_manager_comp.actor_ref().hold().expect("fail");
                let epoch_manager =
                    EpochManager::new(arcon_conf.epoch_interval, source_manager_ref);
                Some(system.create(|| epoch_manager))
            }
            ExecutionMode::Distributed => None,
        };

        let timeout = std::time::Duration::from_millis(500);

        system
            .start_notify(&state_manager_comp)
            .wait_timeout(timeout)
            .expect("StateManager comp never started!");

        system
            .start_notify(&source_manager_comp)
            .wait_timeout(timeout)
            .expect("SourceManager comp never started!");

        (
            system,
            state_manager_comp,
            epoch_manager,
            source_manager_comp,
        )
    }

    /// Helper function to set up internals of the pipeline
    #[cfg(feature = "arcon_tui")]
    fn setup(
        arcon_conf: &ArconConf,
    ) -> (
        KompactSystem,
        Arc<Component<StateManager>>,
        Option<Arc<Component<EpochManager>>>,
        Arc<Component<SourceManager>>,
        Arc<Component<TuiComponent>>,
        arcon_tui::Receiver<TuiNode>,
    ) {
        let kompact_config = arcon_conf.kompact_conf();
        let system = kompact_config.build().expect("KompactSystem");

        let state_manager = StateManager::new();
        let state_manager_comp = system.create_dedicated(|| state_manager);
        let timeout = std::time::Duration::from_millis(500);
        system
            .start_notify(&state_manager_comp)
            .wait_timeout(timeout)
            .expect("StateManager comp never started!");

        let source_manager = SourceManager::new();
        let source_manager_comp = system.create(|| source_manager);

        let epoch_manager = match arcon_conf.execution_mode {
            ExecutionMode::Local => {
                let source_manager_ref = source_manager_comp.actor_ref().hold().expect("fail");
                let epoch_manager =
                    EpochManager::new(arcon_conf.epoch_interval, source_manager_ref);
                Some(system.create(|| epoch_manager))
            }
            ExecutionMode::Distributed => None,
        };

        system
            .start_notify(&source_manager_comp)
            .wait_timeout(timeout)
            .expect("SourceManager comp never started!");

        let (arcon_sender, arcon_receiver) = arcon_tui::unbounded::<TuiNode>();
        let tui_c = TuiComponent::new(arcon_sender);
        let tui_component = system.create_dedicated(|| tui_c);
        system
            .start_notify(&tui_component)
            .wait_timeout(timeout)
            .expect("TuiComponent never started!");
        (
            system,
            state_manager_comp,
            epoch_manager,
            source_manager_comp,
            tui_component,
            arcon_receiver,
        )
    }

    pub fn connect_state_port<B: Backend>(&mut self, nm: &Arc<Component<NodeManager<B>>>) {
        biconnect_components::<StateManagerPort, _, _>(&self.state_manager, nm)
            .expect("connection");
    }

    /// Add component `c` to receive state snapshots from `state_id`
    pub fn watch(
        &mut self,
        state_ids: &'static [&str],
        c: Arc<dyn AbstractComponent<Message = SnapshotRef>>,
    ) {
        self.state_manager.on_definition(|cd| {
            for id in state_ids.into_iter() {
                let state_id = id.to_owned().to_string();

                if !cd.registered_state_ids.contains(&state_id) {
                    panic!(
                        "State id {} has not been registered at the StateManager",
                        state_id
                    );
                }

                let actor_ref = c.actor_ref().hold().expect("fail");

                cd.subscribers
                    .entry(state_id)
                    .or_insert(Vec::new())
                    .push(actor_ref);
            }
        });
    }

    /// Give out a mutable reference to the KompactSystem of the pipeline
    pub fn system(&mut self) -> &mut KompactSystem {
        &mut self.system
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

    /// Awaits termination from the pipeline
    ///
    /// Note that this blocks the current thread
    pub fn await_termination(self) {
        self.system.await_termination();
    }

    /// Shuts the pipeline down and consumes the struct
    pub fn shutdown(self) {
        let _ = self.system.shutdown();
    }

    /// Launches tui dashboard
    #[cfg(feature = "arcon_tui")]
    pub fn tui(&mut self) {
        arcon_tui::tui(self.arcon_event_receiver.clone());
    }
}
