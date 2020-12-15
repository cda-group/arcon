// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    buffer::event::PoolInfo,
    conf::{ArconConf, ExecutionMode},
    dataflow::{
        dfg::{CollectionKind, *},
        stream::Context,
    },
    manager::{epoch::EpochManager, source::SourceManager, state::StateManager},
    prelude::*,
    stream::source::ArconSource,
};
use arcon_allocator::Allocator;
use kompact::{component::AbstractComponent, prelude::KompactSystem};
use std::sync::{Arc, Mutex};

mod assembled;

pub use crate::dataflow::stream::Stream;
pub use assembled::AssembledPipeline;

/// A Pipeline is the starting point of all Arcon applications.
/// It contains all necessary runtime components, configuration,
/// and a custom allocator.
///
/// # Creating a Pipeline
///
/// See [Configuration](ArconConf)
///
/// With the default configuration
/// ```
/// use arcon::prelude::Pipeline;
///
/// let pipeline = Pipeline::default();
/// ```
///
/// With configuration
/// ```
/// use arcon::prelude::{Pipeline, ArconConf};
///
/// let conf = ArconConf {
///     watermark_interval: 2000,
///     ..Default::default()
/// };
/// let pipeline = Pipeline::with_conf(conf);
/// ```
#[derive(Clone)]
pub struct Pipeline {
    /// [`KompactSystem`] for Control Components
    pub(crate) ctrl_system: KompactSystem,
    /// [`KompactSystem`] for Data Processing Components
    pub(crate) data_system: KompactSystem,
    /// Arcon configuration for this pipeline
    pub(crate) conf: ArconConf,
    /// Arcon allocator for this pipeline
    pub(crate) allocator: Arc<Mutex<Allocator>>,
    /// SourceManager component for this pipeline
    pub(crate) source_manager: Arc<Component<SourceManager>>,
    /// EpochManager component for this pipeline
    pub(crate) epoch_manager: Option<Arc<Component<EpochManager>>>,
    /// StateManager component for this pipeline
    pub(crate) state_manager: Arc<Component<StateManager>>,
}

impl Default for Pipeline {
    fn default() -> Self {
        let conf: ArconConf = Default::default();
        Self::new(conf)
    }
}

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
    ///
    /// Returns a [`Stream`] object that users may execute transformations on.
    ///
    /// Example
    /// ```
    /// use arcon::prelude::*;
    /// let stream: Stream<u64> = Pipeline::default()
    ///     .collection((0..100).collect::<Vec<u64>>());
    /// ```
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

    /// Creates a PoolInfo struct to be used by a ChannelStrategy
    pub fn get_pool_info(&self) -> PoolInfo {
        PoolInfo::new(
            self.conf.channel_batch_size,
            self.conf.buffer_pool_size,
            self.conf.buffer_pool_limit,
            self.allocator.clone(),
        )
    }

    /// Shuts the pipeline down and consumes the struct
    pub fn shutdown(self) {
        let _ = self.data_system.shutdown();
        let _ = self.ctrl_system.shutdown();
    }

    /// Give out a mutable reference to the KompactSystem of the pipeline
    pub(crate) fn system(&mut self) -> &mut KompactSystem {
        &mut self.data_system
    }

    pub(crate) fn ctrl_system(&mut self) -> &mut KompactSystem {
        &mut self.ctrl_system
    }

    /// Give out a reference to the ArconConf of the pipeline
    pub(crate) fn arcon_conf(&self) -> &ArconConf {
        &self.conf
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

    // Internal helper method to connect a StateManagerPort between the
    // StateManager and NodeManager.
    pub(crate) fn connect_state_port(&mut self, nm: &Arc<dyn AbstractComponent<Message = Never>>) {
        self.state_manager.on_definition(|scd| {
            nm.on_dyn_definition(|cd| match cd.get_required_port() {
                Some(p) => biconnect_ports(&mut scd.manager_port, p),
                None => panic!("Failed to connect NodeManager port to StateManager"),
            });
        });
    }
}
