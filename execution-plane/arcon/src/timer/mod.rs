// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconNever, Epoch},
    state_backend::StateBackend,
};

pub mod event_timer;

/// This can be used as fake backend for components that don't need one
pub fn none(_state_backend: &mut dyn StateBackend) -> Box<dyn TimerBackend<ArconNever>> {
    Box::new(())
}

/// Produce a hash wheel based timer backend of the appropriate type
pub fn wheel<T: event_timer::TimerTypeBounds>(
    state_backend: &mut dyn StateBackend,
) -> Box<dyn TimerBackend<T>> {
    let wheel = event_timer::EventTimer::new(state_backend);
    Box::new(wheel)
}

/// API For Timer Implementations
pub trait TimerBackend<E>: Send {
    /// Basic scheduling function
    ///
    /// Returns Ok if the entry was schedulled successfully
    /// or `Err(entry)` if it has already expired.
    fn schedule_after(
        &mut self,
        delay: u64,
        entry: E,
        state_backend: &mut dyn StateBackend,
    ) -> Result<(), E>;

    /// Schedule at a specific time in the future
    ///
    /// Returns Ok if the entry was schedulled successfully
    /// or `Err(entry)` if it has already expired.
    fn schedule_at(
        &mut self,
        time: u64,
        entry: E,
        state_backend: &mut dyn StateBackend,
    ) -> Result<(), E>;

    /// Returns the current time value of the timer
    fn current_time(&mut self, state_backend: &mut dyn StateBackend) -> u64;

    /// Move the timer to the given timestamp, triggering all scheduled events between it and the previous timestamps
    fn advance_to(&mut self, ts: u64, state_backend: &mut dyn StateBackend) -> Vec<E>;

    /// Determines how the timer processes an Epoch marker
    ///
    /// Allows the timer to persist its state, if necessary, for example.
    fn handle_epoch(&mut self, epoch: Epoch, state_backend: &mut dyn StateBackend);
}

impl TimerBackend<ArconNever> for () {
    fn schedule_after(
        &mut self,
        _delay: u64,
        _entry: ArconNever,
        _state_backend: &mut dyn StateBackend,
    ) -> Result<(), ArconNever> {
        unreachable!(ArconNever::IS_UNREACHABLE);
    }

    fn schedule_at(
        &mut self,
        _time: u64,
        _entry: ArconNever,
        _state_backend: &mut dyn StateBackend,
    ) -> Result<(), ArconNever> {
        unreachable!(ArconNever::IS_UNREACHABLE);
    }

    fn current_time(&mut self, _state_backend: &mut dyn StateBackend) -> u64 {
        unimplemented!("No point in calling this if you don't need timers anyway.");
    }

    fn advance_to(&mut self, _ts: u64, _state_backend: &mut dyn StateBackend) -> Vec<ArconNever> {
        Vec::new()
    }
    fn handle_epoch(&mut self, _epoch: Epoch, _state_backend: &mut dyn StateBackend) {
        () // do absolutely nothing
    }
}
