// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

//! A runtime for writing streaming applications in the Rust programming language.
//!
//! # Example
//!
//! ```no_run
//! use arcon::prelude::*;
//!
//! let mut pipeline = Pipeline::default()
//!     .collection((0..100).collect::<Vec<u64>>(), |conf| {
//!         conf.set_arcon_time(ArconTime::Process);
//!     })
//!     .operator(OperatorBuilder {
//!         constructor: Arc::new(|_| Filter::new(|x| *x > 50)),
//!         conf: Default::default(),
//!      })
//!     .to_console()
//!     .build();
//!
//! pipeline.start();
//! pipeline.await_termination();
//! ```
//!
//! # Feature Flags
//!
//! - `rocksdb`
//!     - Enables RocksDB to be used as a Backend
//! - `arcon_serde`
//!     - Adds serde support for Arcon Types

#![feature(unboxed_closures)]
#![feature(unsized_fn_params)]
#![feature(core_intrinsics)]

// Enable use of arcon_macros within this crate
#[cfg_attr(test, macro_use)]
extern crate arcon_macros;
extern crate self as arcon;

#[doc(hidden)]
pub use arcon_macros::*;
#[doc(hidden)]
pub use arcon_state::*;

pub use crate::index::{ArconState, IndexOps};
pub use arcon_state::error::ArconStateError;

// Imports below are exposed for ``arcon_macros``

#[doc(hidden)]
pub use crate::data::{ArconType, VersionId};
#[doc(hidden)]
pub use crate::{
    data::arrow::ToArrow,
    table::{ImmutableTable, MutableTable, RecordBatchBuilder, RECORD_BATCH_SIZE},
};
#[doc(hidden)]
pub use arrow::{
    array::{
        ArrayBuilder, ArrayData, ArrayDataBuilder, PrimitiveBuilder, StringBuilder, StructArray,
        StructBuilder,
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

/// Arcon buffer implementations
mod buffer;
/// Arcon Configuration
mod conf;
/// Arcon data types, serialisers/deserialisers
mod data;
/// Dataflow API
mod dataflow;
mod index;
/// Module containing different runtime managers
mod manager;
#[cfg(feature = "metrics")]
#[allow(dead_code)]
/// Arcon metrics
mod metrics;
/// Utilities for creating an Arcon pipeline
mod pipeline;
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
    use once_cell::sync::Lazy;
    use std::sync::{Arc, Mutex};

    pub static ALLOCATOR: Lazy<Arc<Mutex<Allocator>>> =
        Lazy::new(|| Arc::new(Mutex::new(Allocator::new(1073741824))));

    pub fn temp_backend() -> arcon_state::Sled {
        use arcon_state::backend::Backend;
        let test_dir = tempfile::tempdir().unwrap();
        let path = test_dir.path();
        arcon_state::Sled::create(path).unwrap()
    }
}

pub mod client {
    pub use crate::manager::query::{messages::*, QUERY_MANAGER_NAME};
}

/// Helper module that imports everything related to arcon into scope
pub mod prelude {
    /*
    #[cfg(feature = "socket")]
    pub use crate::stream::{
        operator::sink::socket::SocketSink,
        source::socket::{SocketKind, SocketSource},
    };
    */
    pub use crate::{
        conf::ArconConf,
        data::{ArconElement, ArconNever, ArconType, StateID, VersionId},
        dataflow::conf::{
            OperatorBuilder, OperatorConf, ParallelismStrategy, SourceBuilder, SourceConf,
            StreamKind,
        },
        manager::snapshot::Snapshot,
        pipeline::{AssembledPipeline, Pipeline, Stream},
        stream::{
            operator::{
                function::{Filter, FlatMap, Map, MapInPlace},
                sink::local_file::LocalFileSink,
                window::{AppenderWindow, IncrementalWindow, WindowAssigner},
                Operator, OperatorContext,
            },
            source::Source,
            time::{ArconTime, Time},
        },
        Arcon, ArconState,
    };

    #[cfg(feature = "kafka")]
    pub use crate::stream::source::kafka::KafkaConsumerConf;
    #[cfg(feature = "kafka")]
    pub use rdkafka::config::ClientConfig;
    //pub use crate::stream::operator::sink::kafka::KafkaSink;

    pub use arcon_error::{arcon_err, arcon_err_kind, ArconResult, OperatorResult};

    #[doc(hidden)]
    pub use kompact::{
        default_components::*,
        prelude::{Channel as KompactChannel, *},
    };

    #[cfg(feature = "thread_pinning")]
    pub use kompact::{get_core_ids, CoreId};

    pub use super::{Arrow, MutableTable, ToArrow};
    pub use arrow::util::pretty;
    pub use datafusion::prelude::*;

    pub use arcon_state as state;

    pub use arcon_state::{
        Aggregator, AggregatorState, Backend, BackendType, Handle, MapState, ReducerState, Sled,
        ValueState, VecState,
    };

    pub use crate::index::{
        AppenderIndex, EagerAppender, EagerHashTable, EagerValue, EmptyState, HashTable, IndexOps,
        LazyValue, LocalValue, StateConstructor, Timer as ArconTimer, ValueIndex,
    };

    pub use prost::*;

    #[cfg(feature = "rayon")]
    pub use rayon::prelude::*;

    pub use std::sync::Arc;
}
