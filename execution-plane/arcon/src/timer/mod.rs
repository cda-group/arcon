// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::data::ArconNever;

pub mod event_timer;

/// This can be used as fake backend for components that don't need one
pub fn none() -> Box<dyn TimerBackend<ArconNever>> {
    Box::new(())
}

/// Produce a hash wheel based timer backend of the appropriate type
pub fn wheel<T>() -> Box<dyn TimerBackend<T>>
where
    T: prost::Message + Default + PartialEq + 'static,
{
    let wheel = event_timer::EventTimer::default();
    Box::new(wheel)
}

/// API For Timer Implementations
pub trait TimerBackend<E>: Send {
    /// Basic scheduling function
    ///
    /// Returns Ok if the entry was schedulled successfully
    /// or `Err(entry)` if it has already expired.
    fn schedule_after(&mut self, delay: u64, entry: E) -> Result<(), E>;

    /// Schedule at a specific time in the future
    ///
    /// Returns Ok if the entry was schedulled successfully
    /// or `Err(entry)` if it has already expired.
    fn schedule_at(&mut self, time: u64, entry: E) -> Result<(), E>;

    /// Returns the current time value of the timer
    fn current_time(&self) -> u64;

    /// Move the timer to the given timestamp, triggering all scheduled events between it and the previous timestamps
    fn advance_to(&mut self, ts: u64) -> Vec<E>;
}

impl TimerBackend<ArconNever> for () {
    fn schedule_after(&mut self, _delay: u64, _entry: ArconNever) -> Result<(), ArconNever> {
        unreachable!(ArconNever::IS_UNREACHABLE);
    }

    fn schedule_at(&mut self, _time: u64, _entry: ArconNever) -> Result<(), ArconNever> {
        unreachable!(ArconNever::IS_UNREACHABLE);
    }

    fn current_time(&self) -> u64 {
        unimplemented!("No point in calling this if you don't need timers anyway.");
    }

    fn advance_to(&mut self, _ts: u64) -> Vec<ArconNever> {
        Vec::new()
    }
}
