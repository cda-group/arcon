// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

//! Arcon is a Streaming-first Analytics Engine.

#![feature(unboxed_closures)]
#![feature(unsized_fn_params)]

// Enable use of arcon_macros within this crate
#[cfg_attr(test, macro_use)]
extern crate arcon_macros;
extern crate self as arcon;

#[doc(hidden)]
pub use arcon_macros::*;
#[doc(hidden)]
pub use arcon_state as state;

// Imports below are exposed for #[derive(Arcon)]
#[doc(hidden)]
pub use crate::data::{ArconType, VersionId};
#[doc(hidden)]
pub use fxhash::FxHasher;
#[doc(hidden)]
pub use kompact::prelude::SerId;
#[doc(hidden)]
pub use twox_hash::XxHash64;

// Public Interface

/// Arcon Configuration
pub mod conf;
/// Utilities for creating an Arcon pipeline
pub mod pipeline;

// Internal modules

/// Arcon buffer implementations
mod buffer;
/// Arcon data types, serialisers/deserialisers
mod data;
/// Module containing different runtime managers
mod manager;
#[cfg(feature = "metrics")]
/// Arcon metrics
mod metrics;
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
    #[cfg(feature = "socket")]
    pub use crate::stream::{
        operator::sink::socket::SocketSink,
        source::socket::{SocketKind, SocketSource},
    };
    pub use crate::{
        buffer::event::{BufferPool, BufferReader, BufferWriter, PoolInfo},
        conf::ArconConf,
        data::VersionId,
        pipeline::Pipeline,
        stream::{
            channel::{
                strategy::{
                    broadcast::Broadcast, forward::Forward, key_by::KeyBy, round_robin::RoundRobin,
                    ChannelStrategy,
                },
                Channel,
            },
            node::{debug::DebugNode, Node, NodeDescriptor, NodeState},
            operator::{
                function::{Filter, FlatMap, Map, MapInPlace},
                sink::local_file::LocalFileSink,
                Operator, OperatorContext,
            },
            source::{collection::CollectionSource, local_file::LocalFileSource},
        },
    };

    #[cfg(feature = "kafka")]
    pub use crate::stream::operator::sink::kafka::KafkaSink;

    pub use crate::data::{
        flight_serde::{reliable_remote::ReliableSerde, unsafe_remote::UnsafeSerde, FlightSerde},
        *,
    };
    pub use arcon_error::{arcon_err, arcon_err_kind, ArconResult};

    pub use kompact::{default_components::*, prelude::*};
    #[cfg(feature = "thread_pinning")]
    pub use kompact::{get_core_ids, CoreId};

    pub use arcon_state as state;

    pub use arcon_state::{
        AggregatorState, Appender, ArconState, Backend, BackendNever, EagerAppender, Handle,
        MapState, ReducerState, Sled, Value, ValueState, VecState,
    };

    #[cfg(feature = "rayon")]
    pub use rayon::prelude::*;
}
