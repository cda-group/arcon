// Copyright (c) 2021, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{ArconResult, Error};
use snafu::Snafu;
use std::fmt::Debug;

/// TimerResult type utilised while scheduling timers
pub type TimerResult<A> = ArconResult<std::result::Result<(), TimerExpiredError<A>>>;

#[derive(Debug, Snafu)]
#[snafu(display(
    "Attempted to schedule timer entry {:?} at {} when time is {}",
    entry,
    scheduled_time,
    current_time
))]
pub struct TimerExpiredError<A: Debug> {
    /// Current event time
    pub current_time: u64,
    /// The scheduled time
    pub scheduled_time: u64,
    /// Timer Entry
    pub entry: A,
}

impl<A: Debug> From<Error> for TimerResult<A> {
    fn from(error: Error) -> Self {
        Err(error)
    }
}
