// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
// Based off: https://github.com/infusionsoft/yammer-metrics/blob/master/metrics-core/src/main/java/com/codahale/metrics/EWMA.java

const INTERVAL: isize = 5;
const SECONDS_PER_MINUTE: f64 = 60.0;
const ONE_MINUTE: usize = 1;
const FIVE_MINUTES: usize = 5;
const FIFTEEN_MINUTES: usize = 15;
/// One-min Rate
const M1_ALPHA: f64 = 1.0 - (-INTERVAL as f64 / SECONDS_PER_MINUTE / ONE_MINUTE as f64);
/// Five-min Rate
const M5_ALPHA: f64 = 1.0 - (-INTERVAL as f64 / SECONDS_PER_MINUTE / FIVE_MINUTES as f64);
/// Fifteen-min Rate
const M15_ALPHA: f64 = 1.0 - (-INTERVAL as f64 / SECONDS_PER_MINUTE / FIFTEEN_MINUTES as f64);

/// Exponentially Weighted Moving Average
#[derive(Clone, Debug)]
pub struct EWMA {
    initialised: bool,
    rate: f64,
    uncounted: u64,
    alpha: f64,
    interval: f64,
}

impl EWMA {
    #[inline]
    pub fn new(alpha: f64, interval: f64) -> EWMA {
        EWMA {
            initialised: false,
            rate: 0.0,
            uncounted: 0,
            alpha,
            interval,
        }
    }
    #[inline]
    pub fn one_min_ewma() -> EWMA {
        EWMA::new(M1_ALPHA, INTERVAL as f64)
    }

    #[inline]
    pub fn five_min_ewma() -> EWMA {
        EWMA::new(M5_ALPHA, INTERVAL as f64)
    }

    #[inline]
    pub fn fifteen_min_ewma() -> EWMA {
        EWMA::new(M15_ALPHA, INTERVAL as f64)
    }

    #[inline]
    pub fn update(&mut self, n: u64) {
        self.uncounted += n;
    }

    #[inline]
    pub fn tick(&mut self) {
        let count = self.uncounted;
        self.uncounted = 0; // reset
        let instant_rate: f64 = count as f64 / self.interval;

        if self.initialised {
            self.rate += (self.alpha * (instant_rate - self.rate));
        } else {
            self.rate = instant_rate;
            self.initialised = true;
        }
    }

    #[inline]
    pub fn get_rate(&self) -> f64 {
        // TODO: is more needed?
        self.rate
    }
}
