// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

//! Arcon is a Streaming Analytics Engine.
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
//!     .filter(|x| *x > 50)
//!     .map(|x| x + 10)
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

// Enable use of arcon_macros within this crate
#[cfg_attr(test, macro_use)]
extern crate arcon_macros;
extern crate self as arcon;

#[doc(hidden)]
pub use arcon_macros::*;
#[doc(hidden)]
pub use arcon_state::*;

pub use arcon_state::{error::ArconStateError, index::ArconState, IndexOps};

// Imports below are exposed for #[derive(Arcon)]
#[doc(hidden)]
pub use crate::data::{ArconType, VersionId};
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
        dataflow::conf::{OperatorConf, SourceConf},
        manager::snapshot::Snapshot,
        pipeline::{AssembledPipeline, Pipeline, Stream},
        stream::{
            operator::{
                function::{Filter, FlatMap, Map, MapInPlace},
                sink::local_file::LocalFileSink,
                Operator, OperatorContext,
            },
            source::collection::CollectionSource,
            time::ArconTime,
        },
        Arcon,
    };

    #[cfg(feature = "kafka")]
    pub use crate::stream::operator::sink::kafka::KafkaSink;

    pub use arcon_error::{arcon_err, arcon_err_kind, ArconResult, OperatorResult};

    #[doc(hidden)]
    pub use kompact::{default_components::*, prelude::*};
    #[cfg(feature = "thread_pinning")]
    pub use kompact::{get_core_ids, CoreId};

    pub use arcon_state as state;

    pub use arcon_state::{
        AggregatorState, Appender, ArconState, Backend, BackendNever, BackendType, EagerAppender,
        Handle, MapState, ReducerState, Sled, Value, ValueState, VecState,
    };

    #[cfg(feature = "rayon")]
    pub use rayon::prelude::*;
}
