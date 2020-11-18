// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

//! `arcon_state` contains all state management related functionality for the arcon system.
//!
//! The crate provides a set of active in-memory state indexes that are backed by
//! state backend implementations [Backend]. Sled is used as the default state backend,
//! but `arcon_state` also supports RocksDB for production use cases.
//!
//! Users may add their own custom indexes as long as  the [IndexOps] trait is implemented.

#![feature(associated_type_defaults)]
#![feature(const_generics)]
#![feature(core_intrinsics)]

/// State Backend Implementations
pub mod backend;
/// State Trait types
pub mod data;
/// Error utilities
pub mod error;
/// Available Active State Indexes
pub mod index;

#[doc(hidden)]
pub use crate::backend::*;
pub use crate::index::{Appender, EagerAppender, EagerMap, IndexOps, Map, TimerIndex, Value};

#[cfg(feature = "arcon_state_derive")]
extern crate arcon_state_derive;
#[cfg(feature = "arcon_state_derive")]
#[doc(hidden)]
pub use arcon_state_derive::*;
