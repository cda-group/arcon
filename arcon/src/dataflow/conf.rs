use super::builder::Assigner;
use crate::{data::ArconType, stream::time::ArconTime};
use std::sync::Arc;

#[cfg(all(feature = "hardware_counters", target_os = "linux"))]
use crate::metrics::perf_event::PerfEvents;

// Defines a Default State Backend for high-level operators that do not use any
// custom-defined state but still need a backend defined for internal runtime state.
cfg_if::cfg_if! {
    if #[cfg(feature = "rocksdb")]  {
        #[cfg(not(test))]
        pub type DefaultBackend = arcon_state::Rocks;
        #[cfg(test)]
        pub type DefaultBackend = arcon_state::Sled;
    } else {
        pub type DefaultBackend = arcon_state::Sled;
    }
}

/// Defines how the runtime will manage the
/// parallelism for a specific Arcon Operator.
#[derive(Copy, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize))]
pub enum ParallelismStrategy {
    /// Use a static number of Arcon nodes
    Static(usize),
    /// Tells the runtime to manage the parallelism
    Managed,
}

impl Default for ParallelismStrategy {
    fn default() -> Self {
        // static for now until managed is complete and stable..
        ParallelismStrategy::Static(1)
    }
}

/// Defines whether a stream is Keyed or Local
///
/// Streams are by default Keyed in Arcon.
#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize))]
pub enum StreamKind {
    Keyed,
    Local,
}

impl Default for StreamKind {
    fn default() -> Self {
        StreamKind::Keyed
    }
}

/// Operator Configuration
///
/// Defines how an Operator is to be executed on Arcon.
#[derive(Default, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize))]
pub struct OperatorConf {
    /// Parallelism Strategy for this Operator
    pub parallelism_strategy: ParallelismStrategy,
    /// Defines the type of Stream, by default streams are Keyed in Arcon.
    pub stream_kind: StreamKind,
    #[cfg(all(feature = "hardware_counters", target_os = "linux"))]
    pub perf_events: PerfEvents,
}

impl OperatorConf {
    /// Load an OperatorConf from a File using the Hocon format
    #[cfg(all(feature = "serde", feature = "hocon"))]
    pub fn from_file(path: impl AsRef<std::path::Path>) -> OperatorConf {
        // okay to panic here as this is during setup code...
        let data = std::fs::read_to_string(path).unwrap();
        hocon::HoconLoader::new()
            .load_str(&data)
            .unwrap()
            .resolve()
            .unwrap()
    }
}

pub type TimestampExtractor<A> = Arc<dyn Fn(&A) -> u64 + Send + Sync>;

/// Source Configuration
#[derive(Clone)]
pub struct SourceConf<S: ArconType> {
    pub extractor: Option<TimestampExtractor<S>>,
    pub time: ArconTime,
    pub batch_size: usize,
    pub name: String,
}

impl<S: ArconType> SourceConf<S> {
    /// Set [ArconTime] to be used for a Source
    pub fn set_arcon_time(&mut self, time: ArconTime) {
        self.time = time;
    }
    /// Set a Timestamp Extractor for a Source
    pub fn set_timestamp_extractor(&mut self, f: impl Fn(&S) -> u64 + Send + Sync + 'static) {
        self.extractor = Some(Arc::new(f));
    }
    // Set batch size per process iteration
    pub fn set_batch_size(&mut self, size: usize) {
        self.batch_size = size;
    }

    pub fn set_source_name(&mut self, name: String) {
        self.name = name;
    }
}

impl<S: ArconType> Default for SourceConf<S> {
    fn default() -> Self {
        Self {
            extractor: None,
            time: Default::default(),
            batch_size: 1024,
            name: format!("source_{}", uuid::Uuid::new_v4()),
        }
    }
}

#[derive(Clone, Copy)]
pub struct WindowConf {
    pub assigner: Assigner,
}
