//! `arcon_state` contains all state management related functionality for the arcon system.

/// State Backend Implementations
pub mod backend;
/// State Trait types
pub mod data;
/// Error utilities
pub mod error;

#[doc(hidden)]
pub use crate::backend::*;
