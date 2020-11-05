// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

/// A monotonically increasing counter metric
#[derive(Debug, Clone)]
pub struct Counter {
    counter: u128,
}

impl Counter {
    /// Create new Counter
    #[inline]
    pub fn new() -> Counter {
        Counter { counter: 0 }
    }
    /// Increment the Counter by one
    #[inline]
    pub fn inc(&mut self) {
        self.inc_n(1);
    }

    /// Increment the Counter by n
    #[inline]
    pub fn inc_n(&mut self, n: usize) {
        self.counter += n as u128;
    }
}
