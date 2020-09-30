// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

#![feature(associated_type_defaults)]
#![feature(const_generics)]
#![feature(core_intrinsics)]

/// State Backend Implementations
pub mod backend;
/// Error utilities
pub mod error;
/// Available Active State Indexes
pub mod index;

/// State Trait types
mod data;

#[doc(hidden)]
pub use crate::backend::*;
#[doc(hidden)]
pub use crate::data::{Key, Metakey, Value};

#[cfg(feature = "arcon_state_derive")]
extern crate arcon_state_derive;
#[cfg(feature = "arcon_state_derive")]
#[doc(hidden)]
pub use arcon_state_derive::*;
