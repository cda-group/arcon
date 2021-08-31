// Copyright (c) 2021, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

//! A runtime for writing real-time analytics applications in the Rust programming language.
//!
//! # Example
//!
//! ```no_run
//! use arcon::prelude::*;
//!
//! let mut app = Application::default()
//!     .iterator(0..100, |conf| {
//!         conf.set_arcon_time(ArconTime::Process);
//!     })
//!     .filter(|x| *x > 50)
//!     .to_console()
//!     .build();
//!
//! app.start();
//! app.await_termination();
//! ```
//! # Feature Flags
//!
//! - `rocksdb`
//!     - Enables RocksDB to be used as a Backend
//! - `arcon_serde`
//!     - Adds serde support for Arcon Types
//! - `metrics'
//!     - Records internal runtime metrics and allows users to register custom metrics from an Operator
//!     - If no exporter (e.g., prometheus_exporter) is enabled, the metrics will be logged by the runtime.
//! - `hardware_counters`
//!     - Enables counters like cache misses, branch misses etc.
//!     - It is to be noted that this feature is only compatible with linux OS as it uses perf_event_open() under the hood
//!     - One has to provide CAP_SYS_ADMIN capability to use it for eg:  setcap cap_sys_admin+ep target/debug/collection , this takes the built file as an argument.
//!     - Not executing the above command will result into "Operation not permitted" error assuming the feature flag is enabled.
//! - `prometheus_exporter`
//!     - If this flag is enabled , one can see the metrics using the prometheus scrape endpoint assuming there is a running prometheus instance.
//!     - One has to add a target to prometheus config:
//!     - job_name: 'metrics-exporter-prometheus-http'
//!         scrape_interval: 1s
//!          static_configs:
//!           - targets: ['localhost:9000']
//!
//! - `allocator_metrics`
//!     - With this feature on, the runtime will record allocator metrics (e.g., total_bytes, bytes_remaining, alloc_counter).
//!
//! - `state_metrics`
//!     - With this feature on, the runtime will record various state metrics (e.g., bytes in/out, last checkpoint size).
//!
//!

// Enable use of arcon_macros within this crate
#[cfg_attr(test, macro_use)]
extern crate arcon_macros;
extern crate self as arcon;

#[doc(hidden)]
pub use arcon_macros::*;
#[doc(hidden)]
pub use arcon_state::*;

// Imports below are exposed for ``arcon_macros``

#[doc(hidden)]
pub use crate::index::{ArconState, IndexOps};
#[doc(hidden)]
pub use arcon_state::error::ArconStateError;

#[doc(hidden)]
pub use crate::data::{ArconType, VersionId};
#[doc(hidden)]
pub use crate::{
    data::arrow::ToArrow,
    error::ArconResult,
    table::{ImmutableTable, MutableTable, RecordBatchBuilder, RECORD_BATCH_SIZE},
};
#[doc(hidden)]
pub use arrow::{
    array::{
        ArrayBuilder, ArrayData, ArrayDataBuilder, PrimitiveBuilder, StringBuilder, StructArray,
        StructBuilder, UInt64Array,
    },
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
};
#[doc(hidden)]
pub use fxhash::FxHasher;
#[doc(hidden)]
pub use kompact::prelude::SerId;
#[doc(hidden)]
pub use twox_hash::XxHash64;

// exposed for benching
pub mod bench_utils {
    pub use crate::buffer::event::{BufferPool, BufferReader};
}

/// Utilities for creating an Arcon Application
mod application;
/// Arcon buffer implementations
mod buffer;
/// Arcon data types, serialisers/deserialisers
mod data;
/// Dataflow API
mod dataflow;
/// Arcon Error types
pub mod error;
/// Arcon State Indexes
mod index;
/// Module containing different runtime managers
mod manager;
#[cfg(feature = "metrics")]
#[allow(dead_code)]
/// Arcon metrics
mod metrics;
/// Contains the core stream logic
mod stream;
/// Table implementations
mod table;
/// Test module containing some more complex unit tests
#[cfg(test)]
mod test;
/// Internal Arcon Utilities
mod util;

/// A module containing test utilities such as a global Arcon Allocator
#[cfg(test)]
pub mod test_utils {
    use arcon_allocator::Allocator;
    use arcon_state::backend::Backend;
    use once_cell::sync::Lazy;
    use std::sync::{Arc, Mutex};

    pub static ALLOCATOR: Lazy<Arc<Mutex<Allocator>>> =
        Lazy::new(|| Arc::new(Mutex::new(Allocator::new(1073741824))));

    pub fn temp_backend<B: Backend>() -> B {
        let test_dir = tempfile::tempdir().unwrap();
        let path = test_dir.path();
        B::create(path, "testDB".to_string()).unwrap()
    }
}

/// Helper module that imports everything related to arcon into scope
pub mod prelude {
    pub use crate::{
        application::conf::{logger::LoggerType, ApplicationConf},
        application::{Application, AssembledApplication, Stream},
        data::{ArconElement, ArconNever, ArconType, StateID, VersionId},
        dataflow::{
            api::{Assigner, OperatorBuilder, SourceBuilder},
            conf::{OperatorConf, ParallelismStrategy, SourceConf, StreamKind, WindowConf},
        },
        manager::snapshot::Snapshot,
        stream::{
            operator::{
                function::{Filter, FlatMap, Map, MapInPlace},
                sink::local_file::LocalFileSink,
                window::{WindowAssigner, WindowState},
                Operator, OperatorContext,
            },
            source::{schema::ProtoSchema, Source},
            time::{ArconTime, Time},
        },
        Arcon, ArconState,
    };

    #[cfg(feature = "kafka")]
    pub use crate::stream::source::kafka::KafkaConsumerConf;
    #[cfg(feature = "serde_json")]
    pub use crate::stream::source::schema::JsonSchema;
    #[cfg(feature = "kafka")]
    pub use rdkafka::config::ClientConfig;

    pub use crate::error::{timer::TimerExpiredError, ArconResult, StateResult};

    #[doc(hidden)]
    pub use kompact::{
        default_components::*,
        prelude::{Channel as KompactChannel, *},
    };

    #[cfg(feature = "thread_pinning")]
    pub use kompact::{get_core_ids, CoreId};

    pub use super::{Arrow, MutableTable, ToArrow};
    pub use arrow::{datatypes::Schema, record_batch::RecordBatch, util::pretty};
    pub use datafusion::{datasource::MemTable, prelude::*};

    pub use arcon_state as state;

    #[cfg(feature = "rocksdb")]
    pub use arcon_state::Rocks;
    pub use arcon_state::{
        Aggregator, AggregatorState, Backend, BackendType, Handle, MapState, ReducerState, Sled,
        ValueState, VecState,
    };

    pub use crate::index::{
        timer, AppenderIndex, AppenderWindow, ArrowWindow, EagerAppender, EagerHashTable,
        EagerValue, EmptyState, HashTable, IncrementalWindow, IndexOps, LazyValue, LocalValue,
        ValueIndex,
    };

    pub use prost::*;

    #[cfg(feature = "rayon")]
    pub use rayon::prelude::*;

    pub use std::sync::Arc;

    #[cfg(all(feature = "hardware_counters", target_os = "linux"))]
    pub use crate::metrics::perf_event::{HardwareCounter, PerfEvents};
}
