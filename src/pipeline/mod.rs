// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    buffer::event::PoolInfo,
    conf::{ArconConf, ExecutionMode},
    dataflow::{
        dfg::{CollectionKind, *},
        stream::Context,
    },
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
use arcon_state::index::ArconState;
use kompact::{component::AbstractComponent, prelude::KompactSystem};
use std::sync::{
    mpsc,
    mpsc::{Receiver, Sender},
    Arc, Mutex,
};

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

pub type ErasedManager = Box<dyn CreateErased<Never>>;
pub type ErasedNode<IN> = Box<dyn CreateErased<ArconMessage<IN>>>;

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

    /// Spawns a new thread to run the function `F` on
    /// an ArconState object defined through its `StateID` per epoch.
    pub fn watch<S, F>(&mut self, state_id: impl Into<StateID>, f: F)
    where
        S: ArconState + std::convert::From<SnapshotRef>,
        F: Fn(u64, S) + Send + Sync + 'static,
    {
        let (tx, rx): (Sender<SnapshotRef>, Receiver<SnapshotRef>) = mpsc::channel();
        std::thread::spawn(move || loop {
            let snapshot_ref = rx.recv().unwrap();
            let epoch = snapshot_ref.snapshot.epoch;
            let state: S = snapshot_ref.into();
            f(epoch, state);
        });

        self.state_manager.on_definition(|cd| {
            let state_id = state_id.into();
            if !cd.registered_state_ids.contains(&state_id) {
                panic!(
                    "State id {} has not been registered at the StateManager",
                    state_id
                );
            }
            cd.channels.insert(state_id, tx);
        });
    }

    /// Add component `c` to receive state snapshots from `state_id`
    pub fn watch_with(
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
    fn create_node_manager(
        &mut self,
        id: String,
        in_channels: Vec<NodeID>,
        backend: Arc<impl Backend>,
    ) -> ErasedManager {
        Box::new(NodeManager::new(id, in_channels, backend))
    }

    /// Creates a bounded data source using a local file
    pub fn file<I, A>(self, i: I) -> Stream<A>
    where
        I: Into<String>,
        A: ArconType + std::str::FromStr,
    {
        let path = i.into();
        assert_eq!(
            std::path::Path::new(&path).exists(),
            true,
            "File does not exist"
        );

        let mut ctx = Context::new(self);
        let file_kind = LocalFileKind::new(path);
        let kind = DFGNodeKind::Source(SourceKind::LocalFile(file_kind), Default::default());
        let dfg_node = DFGNode::new(kind, Default::default(), vec![]);
        ctx.dfg.insert(dfg_node);

        Stream::new(ctx)
    }

    /// Creates a bounded data source using a Vector of [`ArconType`]
    pub fn collection<I, A>(self, i: I) -> Stream<A>
    where
        I: Into<Vec<A>>,
        A: ArconType,
    {
        let collection_kind = CollectionKind::new(Box::new(i.into()));
        let mut ctx = Context::new(self);
        let kind = DFGNodeKind::Source(SourceKind::Collection(collection_kind), Default::default());
        let dfg_node = DFGNode::new(kind, Default::default(), vec![]);
        ctx.dfg.insert(dfg_node);
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

        // Start epoch manager to begin the injection of
        // epochs into the pipeline.
        if let Some(epoch_manager) = &self.epoch_manager {
            self.ctrl_system
                .start_notify(&epoch_manager)
                .wait_timeout(std::time::Duration::from_millis(500))
                .expect("Failed to start EpochManager");
        }
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

    // Internal helper method to add a source component to the [`SourceManager`]
    pub(crate) fn add_source_comp(
        &mut self,
        comp: Arc<dyn AbstractComponent<Message = ArconSource>>,
    ) {
        self.source_manager.on_definition(|cd| {
            cd.sources.push(comp);
        });
    }

    pub(crate) fn connect_state_porty(&mut self, nm: &Arc<dyn AbstractComponent<Message = Never>>) {
        self.state_manager.on_definition(|scd| {
            nm.on_dyn_definition(|cd| match cd.get_required_port() {
                Some(p) => biconnect_ports(&mut scd.manager_port, p),
                None => panic!("Failed to connect NodeManager port to StateManager"),
            });
        });
    }

    // Internal helper method to connect a NodeManager to the StateManager of the Pipeline
    pub(crate) fn connect_state_port<B: Backend>(&mut self, nm: &Arc<Component<NodeManager<B>>>) {
        biconnect_components::<StateManagerPort, _, _>(&self.state_manager, nm)
            .expect("connection");
    }
}
