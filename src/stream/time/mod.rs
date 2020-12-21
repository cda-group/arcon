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
