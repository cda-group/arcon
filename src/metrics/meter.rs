// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
// Based off: https://github.com/infusionsoft/yammer-metrics/blob/master/metrics-core/src/main/java/com/codahale/metrics/Meter.java

use crate::metrics::ewma::EWMA;
use std::time::Duration;

/// Meter metric measuring throughput in various forms using EWMA
#[derive(Clone, Debug, Default)]
pub struct Meter {
    /// One-min rate
    m1_rate: EWMA,
    /// Five-min rate
    m5_rate: EWMA,
    /// Fifteen-min rate
    m15_rate: EWMA,
    /// Amount of marks
    count: u64,
    /// Time of start
    start_time: u64,
    /// Last time Meter was ticked
    last_tick: u64,
}

impl Meter {
    #[inline]
    pub fn new() -> Meter {
        let start_time = crate::util::get_system_time_nano();
        Meter {
            m1_rate: EWMA::one_min_ewma(),
            m5_rate: EWMA::five_min_ewma(),
            m15_rate: EWMA::fifteen_min_ewma(),
            count: 0,
            start_time,
            last_tick: start_time,
        }
    }

    #[inline]
    pub fn mark(&mut self) {
        self.mark_n(1);
    }

    #[inline]
    pub fn mark_n(&mut self, n: u64) {
        self.tick_if_necessary();
        self.count += n;
        self.m1_rate.update(n);
        self.m5_rate.update(n);
        self.m15_rate.update(n);
    }

    #[inline]
    fn tick_if_necessary(&mut self) {
        let old_tick = self.last_tick;
        // Add clock...
        let new_tick = crate::util::get_system_time_nano();
        let age = new_tick - old_tick;

        let tick_interval = std::time::Duration::new(5, 0).as_nanos() as u64;
        if age > tick_interval {
            let new_interval_tick = new_tick - age % tick_interval;
            self.last_tick = new_interval_tick;
            let required_ticks = age / tick_interval;
            for _ in 0..required_ticks {
                self.m1_rate.tick();
                self.m5_rate.tick();
                self.m15_rate.tick();
            }
        }
    }

    #[inline]
    pub fn get_count(&self) -> u64 {
        self.count
    }

    #[inline]
    pub fn get_one_min_rate(&mut self) -> f64 {
        self.tick_if_necessary();
        self.m1_rate.get_rate()
    }

    #[inline]
    pub fn get_five_min_rate(&mut self) -> f64 {
        self.tick_if_necessary();
        self.m5_rate.get_rate()
    }

    #[inline]
    pub fn get_fifteen_min_rate(&mut self) -> f64 {
        self.tick_if_necessary();
        self.m15_rate.get_rate()
    }

    #[inline]
    pub fn get_mean_rate(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            // Add clock instead..
            let elapsed: f64 = (crate::util::get_system_time_nano() - self.start_time) as f64;
            self.count as f64 / (elapsed * Duration::new(1, 0).as_nanos() as f64)
        }
    }
}
