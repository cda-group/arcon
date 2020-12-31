// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    buffer::event::PoolInfo,
    conf::{ArconConf, ExecutionMode},
    dataflow::{
        conf::{OperatorConf, SourceConf},
        constructor::{source_cons, source_manager_cons},
        dfg::*,
        stream::{Context, DefaultBackend},
    },
    manager::{
        epoch::{EpochEvent, EpochManager},
        snapshot::SnapshotManager,
    },
    prelude::*,
    stream::{
        node::source::SourceEvent,
        source::{local_file::LocalFileSource, Source},
    },
};
use arcon_allocator::Allocator;
use arcon_state::index::EMPTY_STATE_ID;
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
    pub(crate) source_manager: Option<Arc<dyn AbstractComponent<Message = SourceEvent>>>,
    /// EpochManager component for this pipeline
    pub(crate) epoch_manager: Option<Arc<Component<EpochManager>>>,
    /// SnapshotManager component for this pipeline
    pub(crate) snapshot_manager: Arc<Component<SnapshotManager>>,
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
        let (ctrl_system, data_system, snapshot_manager, epoch_manager) = Self::setup(&conf);

        Self {
            ctrl_system,
            data_system,
            conf,
            allocator,
            snapshot_manager,
            epoch_manager,
            source_manager: None,
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
        Arc<Component<SnapshotManager>>,
        Option<Arc<Component<EpochManager>>>,
    ) {
        let data_system = arcon_conf.kompact_conf().build().expect("KompactSystem");
        let ctrl_system = arcon_conf.kompact_conf().build().expect("KompactSystem");

        let snapshot_manager = ctrl_system.create(SnapshotManager::new);

        let epoch_manager = match arcon_conf.execution_mode {
            ExecutionMode::Local => {
                let snapshot_manager_ref = snapshot_manager.actor_ref().hold().expect("fail");
                let epoch_manager =
                    EpochManager::new(arcon_conf.epoch_interval, snapshot_manager_ref);
                Some(ctrl_system.create(|| epoch_manager))
            }
            ExecutionMode::Distributed => None,
        };

        let timeout = std::time::Duration::from_millis(500);

        ctrl_system
            .start_notify(&snapshot_manager)
            .wait_timeout(timeout)
            .expect("SnapshotManager comp never started!");

        (ctrl_system, data_system, snapshot_manager, epoch_manager)
    }

    /// Create a non-parallel data source
    ///
    /// Returns a [`Stream`] object that users may execute transformations on.
    pub fn source<S>(self, source: S, conf: SourceConf<S::Data>) -> Stream<S::Data>
    where
        S: Source,
    {
        assert_ne!(
            conf.time == ArconTime::Event,
            conf.extractor.is_none(),
            "Cannot use ArconTime::Event without specifying a timestamp extractor"
        );
        let cons = source_cons(source, self.get_pool_info());
        let mut state_dir = self.arcon_conf().state_dir.clone();
        state_dir.push("source_manager");
        let backend = Arc::new(DefaultBackend::create(&state_dir).unwrap());
        let manager_cons =
            source_manager_cons(backend, self.arcon_conf().watermark_interval, conf.time);

        let mut ctx = Context::new(self);
        let kind = DFGNodeKind::Source(SourceKind::Single(cons), Default::default(), manager_cons);
        let dfg_node = DFGNode::new(kind, OperatorConf::new(EMPTY_STATE_ID.to_owned()), vec![]);
        ctx.dfg.insert(dfg_node);
        Stream::new(ctx)
    }

    /// Creates a bounded data Stream using a local file
    ///
    /// Returns a [`Stream`] object that users may execute transformations on.
    ///
    /// Example
    /// ```no_run
    /// use arcon::prelude::*;
    /// let stream: Stream<u64> = Pipeline::default()
    ///     .file("/tmp/source_file", |conf| {
    ///         conf.set_arcon_time(ArconTime::Process);
    ///     });
    /// ```
    pub fn file<I, A>(self, i: I, f: impl FnOnce(&mut SourceConf<A>)) -> Stream<A>
    where
        I: Into<String>,
        A: ArconType + std::str::FromStr,
    {
        let path = i.into();
        assert_eq!(
            std::path::Path::new(&path).exists(),
            true,
            "File {} does not exist",
            path
        );
        let mut conf = SourceConf::default();
        f(&mut conf);
        let source = LocalFileSource::new(path, conf.clone());
        self.source(source, conf)
    }

    /// Creates a bounded data Stream using a Collection
    ///
    /// Returns a [`Stream`] object that users may execute transformations on.
    ///
    /// Example
    /// ```
    /// use arcon::prelude::*;
    /// let stream: Stream<u64> = Pipeline::default()
    ///     .collection((0..100).collect::<Vec<u64>>(), |conf| {
    ///         conf.set_arcon_time(ArconTime::Process);
    ///     });
    /// ```
    pub fn collection<I, A>(self, i: I, f: impl FnOnce(&mut SourceConf<A>)) -> Stream<A>
    where
        I: Into<Vec<A>>,
        A: ArconType,
    {
        let collection = i.into();
        let mut conf = SourceConf::default();
        f(&mut conf);
        let source = CollectionSource::new(collection, conf.clone());
        self.source(source, conf)
    }

    // Creates a PoolInfo struct to be used by a ChannelStrategy
    pub fn get_pool_info(&self) -> PoolInfo {
        PoolInfo::new(
            self.conf.channel_batch_size,
            self.conf.buffer_pool_size,
            self.conf.buffer_pool_limit,
            self.allocator.clone(),
        )
    }

    // TODO: Remove
    pub fn shutdown(self) {
        let _ = self.data_system.shutdown();
        let _ = self.ctrl_system.shutdown();
    }

    pub(crate) fn data_system(&mut self) -> &mut KompactSystem {
        &mut self.data_system
    }

    pub(crate) fn ctrl_system(&mut self) -> &mut KompactSystem {
        &mut self.ctrl_system
    }

    /// Give out a reference to the ArconConf of the pipeline
    pub(crate) fn arcon_conf(&self) -> &ArconConf {
        &self.conf
    }

    pub(crate) fn epoch_manager(&self) -> ActorRefStrong<EpochEvent> {
        if let Some(epoch_manager) = &self.epoch_manager {
            epoch_manager
                .actor_ref()
                .hold()
                .expect("Failed to fetch actor ref")
        } else {
            panic!(
                "Only local reference supported for now. should really be an ActorPath later on"
            );
        }
    }
}
