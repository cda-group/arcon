// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

//! Arcon is a Streaming-first Analytics Engine for the Arc language.
//!
//! This crate is not meant to be used directly, but rather relies on
//! [Arc](https://github.com/cda-group/arc) to construct applications and
//! [arcon_codegen] to generate the Rust code.

#![feature(unboxed_closures)]

#[cfg_attr(test, macro_use)]
extern crate arcon_macros;
#[doc(hidden)]
pub use arcon_macros::*;

// Imports below are exposed for #[derive(Arcon)]
pub use crate::data::{ArconType, VersionId};
pub use kompact::prelude::SerId;

#[macro_use]
extern crate arcon_error as error;

// Public Interface

/// Arcon Configuration
pub mod conf;
/// Arcon data types, serialisers/deserialisers
pub mod data;
/// Module containing different runtime managers
pub mod manager;
/// Utilities for creating an Arcon pipeline
pub mod pipeline;
/// State backend implementations
pub mod state_backend;
/// Contains the core stream logic
pub mod stream;
/// Arcon event time facilities
pub mod timer;

// Internal modules

/// Allocator for message buffers, network buffers, state backends
mod allocator;
/// Arcon buffer implementations
mod buffer;
/// Arcon metrics
mod metrics;
/// Test module containing some more complex unit tests
#[cfg(test)]
mod test;
/// Arcon terminal user interface
#[cfg(feature = "arcon_tui")]
mod tui;
/// Internal Arcon Utilities
mod util;

/// A module containing test utilities such as a global ArconAllocator
#[cfg(test)]
pub mod test_utils {
    use crate::allocator::ArconAllocator;
    use once_cell::sync::Lazy;
    use std::sync::{Arc, Mutex};

    pub static ALLOCATOR: Lazy<Arc<Mutex<ArconAllocator>>> =
        Lazy::new(|| Arc::new(Mutex::new(ArconAllocator::new(1073741824))));
}

/// Helper module that imports everything related to arcon into scope
pub mod prelude {
    #[cfg(feature = "socket")]
    pub use crate::stream::{
        operator::sink::socket::SocketSink,
        source::socket::{SocketKind, SocketSource},
    };
    pub use crate::{
        allocator::{AllocResult, ArconAllocator},
        buffer::event::{BufferPool, BufferReader, BufferWriter},
        conf::ArconConf,
        data::VersionId,
        pipeline::ArconPipeline,
        stream::{
            channel::{
                strategy::{
                    broadcast::Broadcast, forward::Forward, key_by::KeyBy, round_robin::RoundRobin,
                    ChannelStrategy,
                },
                Channel, DispatcherSource,
            },
            node::{debug::DebugNode, Node, NodeDescriptor},
            operator::{
                function::{Filter, FilterMap, FlatMap, Map, MapInPlace},
                sink::local_file::LocalFileSink,
                window::{AppenderWindow, EventTimeWindowAssigner, IncrementalWindow, Window},
                Operator,
            },
            source::{collection::CollectionSource, local_file::LocalFileSource, SourceContext},
        },
        timer,
    };
    pub use kompact::prelude::SerId;

    #[cfg(feature = "kafka")]
    pub use crate::stream::{operator::sink::kafka::KafkaSink, source::kafka::KafkaSource};

    pub use crate::data::{
        flight_serde::{reliable_remote::ReliableSerde, unsafe_remote::UnsafeSerde, FlightSerde},
        *,
    };
    pub use error::ArconResult;

    pub use kompact::{default_components::*, prelude::*};
    #[cfg(feature = "thread_pinning")]
    pub use kompact::{get_core_ids, CoreId};

    #[cfg(all(feature = "arcon_faster", target_os = "linux"))]
    pub use crate::state_backend::faster::Faster;
    #[cfg(feature = "arcon_rocksdb")]
    pub use crate::state_backend::rocks::RocksDb;
    #[cfg(feature = "arcon_sled")]
    pub use crate::state_backend::sled::Sled;
    pub use crate::state_backend::{
        builders::*, in_memory::InMemory, state_types::*, StateBackend,
    };
    #[cfg(feature = "rayon")]
    pub use rayon::prelude::*;
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[cfg_attr(feature = "arcon_serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Arcon, prost::Message, Clone, abomonation_derive::Abomonation)]
    #[arcon(unsafe_ser_id = 104, reliable_ser_id = 105, version = 1, keys = "id")]
    pub struct Item {
        #[prost(uint64, tag = "1")]
        pub id: u64,
        #[prost(uint32, tag = "2")]
        pub price: u32,
    }
}
