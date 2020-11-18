// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

/// Gauge metric representing a value that can go up and down
#[derive(Debug, Clone)]
pub struct Gauge {
    value: u128,
}

impl Gauge {
    /// Create new Gauge
    #[inline]
    pub fn new() -> Gauge {
        Gauge { value: 0 }
    }
    /// Increment the Gauge by one
    #[inline]
    pub fn inc(&mut self) {
        self.inc_n(1);
    }

    /// Increment the Gauge by n
    #[inline]
    pub fn inc_n(&mut self, n: usize) {
        self.value += n as u128;
    }

    /// Decrement the Gauge by one
    #[inline]
    pub fn dec(&mut self) {
        self.dec_n(1);
    }

    /// Decrement the Gauge by n
    #[inline]
    pub fn dec_n(&mut self, n: usize) {
        self.value = self.value.checked_sub(n as u128).unwrap_or(0);
    }

    #[inline]
    pub fn get(&self) -> u128 {
        self.value
    }
}
