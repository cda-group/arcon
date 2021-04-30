// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

//! `arcon_state` contains all state management related functionality for the arcon system.

#![feature(associated_type_defaults)]

/// State Backend Implementations
pub mod backend;
/// State Trait types
pub mod data;
/// Error utilities
pub mod error;

#[doc(hidden)]
pub use crate::backend::*;
