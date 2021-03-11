// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

#[derive(PartialEq, Clone, Copy, Debug)]
pub enum ArconTime {
    Event,
    Process,
}

impl Default for ArconTime {
    fn default() -> Self {
        ArconTime::Event
    }
}

pub struct Time(pub u64);

impl Time {
    pub fn seconds(seconds: u64) -> Self {
        Time(seconds)
    }
    pub fn minutes(minutes: u64) -> Self {
        Time(minutes * 60)
    }
    pub fn hours(hours: u64) -> Self {
        Time(hours * 60 * 60)
    }
    pub fn days(days: u64) -> Self {
        Time(days * 24 * 60 * 60)
    }
}
