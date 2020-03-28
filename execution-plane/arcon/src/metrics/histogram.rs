// Based on the histogram implementation found in the sled crate
// Reference: https://github.com/spacejam/sled/blob/master/src/histogram.rs
// Modifications Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

#![allow(unused)]
#![allow(unused_results)]
#![allow(clippy::print_stdout)]
#![allow(clippy::float_arithmetic)]

use std::{
    convert::TryFrom,
    fmt::{self, Debug},
};

const PRECISION: f64 = 100.;
const BUCKETS: usize = 1 << 16;

/// A histogram collector that uses zero-configuration logarithmic buckets.
pub struct Histogram {
    vals: Vec<usize>,
    sum: usize,
    count: usize,
}

impl Default for Histogram {
    fn default() -> Histogram {
        let mut vals = Vec::with_capacity(BUCKETS);
        vals.resize_with(BUCKETS, Default::default);

        Histogram {
            vals,
            sum: 0,
            count: 0,
        }
    }
}

#[allow(unsafe_code)]
unsafe impl Send for Histogram {}

impl Debug for Histogram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        const PS: [f64; 10] = [0., 50., 75., 90., 95., 97.5, 99., 99.9, 99.99, 100.];
        f.write_str("Histogramgram[")?;

        for p in &PS {
            let res = self.percentile(*p).round();
            let line = format!("({} -> {}) ", p, res);
            f.write_str(&*line)?;
        }

        f.write_str("]")
    }
}

impl Histogram {
    /// Record a value.
    #[inline]
    pub fn measure(&mut self, raw_value: u64) {
        let value_float: f64 = raw_value as f64;
        self.sum = value_float.round() as usize;
        self.count += 1;

        // compress the value to one of 2**16 values
        // using logarithmic bucketing
        let compressed: u16 = compress(value_float);

        // increment the counter for this compressed value
        self.vals[compressed as usize] += 1;
    }

    /// Retrieve a percentile [0-100]. Returns NAN if no metrics have been
    /// collected yet.
    pub fn percentile(&self, p: f64) -> f64 {
        assert!(p <= 100., "percentiles must not exceed 100.0");

        let count = self.count;

        if count == 0 {
            return std::f64::NAN;
        }

        let mut target = count as f64 * (p / 100.);
        if target == 0. {
            target = 1.;
        }

        let mut sum = 0.;

        for (idx, val) in self.vals.iter().enumerate() {
            sum += *val as f64;

            if sum >= target {
                return decompress(idx as u16);
            }
        }

        std::f64::NAN
    }

    /// Dump out some common percentiles.
    pub fn print_percentiles(&self) {
        println!("{:?}", self);
    }

    /// Return the sum of all observations in this histogram.
    pub fn sum(&self) -> usize {
        self.sum
    }

    /// Return the count of observations in this histogram.
    pub fn count(&self) -> usize {
        self.count
    }
}

// compress takes a value and lossily shrinks it to an u16 to facilitate
// bucketing of histogram values, staying roughly within 1% of the true
// value. This fails for large values of 1e142 and above, and is
// inaccurate for values closer to 0 than +/- 0.51 or +/- math.Inf.
#[allow(clippy::cast_sign_loss)]
#[allow(clippy::cast_possible_truncation)]
#[inline]
fn compress<T: Into<f64>>(input_value: T) -> u16 {
    let value: f64 = input_value.into();
    let abs = value.abs();
    let boosted = 1. + abs;
    let ln = boosted.ln();
    let compressed = PRECISION.mul_add(ln, 0.5);
    assert!(compressed <= f64::from(u16::max_value()));

    compressed as u16
}

// decompress takes a lossily shrunken u16 and returns an f64 within 1% of
// the original passed to compress.
#[inline]
fn decompress(compressed: u16) -> f64 {
    let unboosted = f64::from(compressed) / PRECISION;
    (unboosted.exp() - 1.)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let mut c = Histogram::default();
        c.measure(2);
        c.measure(2);
        c.measure(3);
        c.measure(3);
        c.measure(4);
        assert_eq!(c.percentile(0.).round() as usize, 2);
        assert_eq!(c.percentile(40.).round() as usize, 2);
        assert_eq!(c.percentile(40.1).round() as usize, 3);
        assert_eq!(c.percentile(80.).round() as usize, 3);
        assert_eq!(c.percentile(80.1).round() as usize, 4);
        assert_eq!(c.percentile(100.).round() as usize, 4);
        c.print_percentiles();
    }

    #[test]
    fn high_percentiles() {
        let mut c = Histogram::default();
        for _ in 0..9000 {
            c.measure(10);
        }
        for _ in 0..900 {
            c.measure(25);
        }
        for _ in 0..90 {
            c.measure(33);
        }
        for _ in 0..9 {
            c.measure(47);
        }
        c.measure(500);
        assert_eq!(c.percentile(0.).round() as usize, 10);
        assert_eq!(c.percentile(99.).round() as usize, 25);
        assert_eq!(c.percentile(99.89).round() as usize, 33);
        assert_eq!(c.percentile(99.91).round() as usize, 47);
        assert_eq!(c.percentile(99.99).round() as usize, 47);
        assert_eq!(c.percentile(100.).round() as usize, 502);
    }
}
