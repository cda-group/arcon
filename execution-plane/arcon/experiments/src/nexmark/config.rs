// Copyright (c) 2017, 2018 ETH Zurich
// SPDX-License-Identifier: MIT
// Modifications Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use anyhow::{Context, Result};
use serde::*;
use serde_repr::*;
use std::f64::consts::PI;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RateShape {
    Square,
    Sine,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NEXMarkConfig {
    pub query: NEXMarkQuery,
    #[serde(default = "num_events")]
    pub num_events: u32,
    #[serde(default = "num_events_generators")]
    pub num_events_generators: u32,
    #[serde(default = "hot_action_ratio")]
    pub hot_auction_ratio: u32,
    #[serde(default = "hot_seller_ratio")]
    pub hot_seller_ratio: u32,
    #[serde(default = "hot_bidder_ratio")]
    pub hot_bidder_ratio: u32,
    #[serde(default = "window_size_sec")]
    pub window_size_sec: u32,
    #[serde(default = "window_period_sec")]
    pub window_period_sec: u32,
    #[serde(default = "num_active_people")]
    pub num_active_people: u32,
    #[serde(default = "in_flight_auctions")]
    pub in_flight_auctions: u32,
    #[serde(default = "stream_timeout")]
    pub stream_timeout: u32,
    #[serde(default = "person_id_lead")]
    pub person_id_lead: u32,
    #[serde(default = "rate_shape")]
    pub rate_shape: RateShape,
    #[serde(default = "num_categories")]
    pub num_categories: u32,
    #[serde(default = "out_of_order_group_size")]
    pub out_of_order_group_size: u32,
    #[serde(default)]
    pub inter_event_delays_ns: Vec<f64>,
    #[serde(default)]
    pub base_time_ns: u32,
    #[serde(default)]
    pub first_event_id: u32,
    #[serde(default)]
    pub first_event_number: u32,
    #[serde(default)]
    pub auction_id_lead: u32,
    #[serde(default)]
    pub hot_seller_ratio_2: u32,
    #[serde(default)]
    pub hot_auction_ratio_2: u32,
    #[serde(default)]
    pub hot_bidder_ratio_2: u32,
    #[serde(default)]
    pub person_proportion: u32,
    #[serde(default)]
    pub auction_proportion: u32,
    #[serde(default)]
    pub bid_proportion: u32,
    #[serde(default)]
    pub proportion_denominator: u32,
    #[serde(default)]
    pub first_auction_id: u32,
    #[serde(default)]
    pub first_person_id: u32,
    #[serde(default)]
    pub first_category_id: u32,
    #[serde(default)]
    pub sine_approx_steps: u32,
    #[serde(default = "us_states")]
    pub us_states: Vec<String>,
    #[serde(default = "us_cities")]
    pub us_cities: Vec<String>,
    #[serde(default = "first_names")]
    pub first_names: Vec<String>,
    #[serde(default = "last_names")]
    pub last_names: Vec<String>,
}

impl NEXMarkConfig {
    pub fn load(path: &str) -> Result<NEXMarkConfig> {
        let data = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config from {}", path))?;
        toml::from_str(&data).map_err(|e| anyhow!("failed to parse toml with err {}", e))
    }

    // TODO: fix hardcoded stuff..
    pub fn finish(conf: &mut NEXMarkConfig) {
        let first_rate = 1000;
        let next_rate = 1000;
        let mut inter_event_delays_ns = Vec::new();
        let ns_per_unit = 1_000_000_000;
        let sine_approx_steps = 10;
        let rate_to_period = |r| (ns_per_unit) as f64 / r as f64;
        if first_rate == next_rate {
            inter_event_delays_ns
                .push(rate_to_period(first_rate) * conf.num_events_generators as f64);
        } else {
            match conf.rate_shape {
                RateShape::Square => {
                    inter_event_delays_ns
                        .push(rate_to_period(first_rate) * conf.num_events_generators as f64);
                    inter_event_delays_ns
                        .push(rate_to_period(next_rate) * conf.num_events_generators as f64);
                }
                RateShape::Sine => {
                    let mid = (first_rate + next_rate) as f64 / 2.0;
                    let amp = (first_rate - next_rate) as f64 / 2.0;
                    for i in 0..sine_approx_steps {
                        let r = (2.0 * PI * i as f64) / sine_approx_steps as f64;
                        let rate = mid + amp * r.cos();
                        inter_event_delays_ns.push(
                            rate_to_period(rate.round() as usize)
                                * conf.num_events_generators as f64,
                        );
                    }
                }
            }
        }

        conf.inter_event_delays_ns = inter_event_delays_ns;
    }

    pub fn event_timestamp_ns(&self, event_number: u32) -> u32 {
        return self.base_time_ns + ((event_number as f64 * self.inter_event_delays_ns[0]) as u32);
    }

    pub fn next_adjusted_event(&self, events_so_far: u32) -> u32 {
        let n = self.out_of_order_group_size;
        let event_number = self.first_event_number + events_so_far;
        (event_number / n) * n + (event_number * 953u32) % n
    }
}

/// Enum containing supported Queries
#[derive(Serialize_repr, Deserialize_repr, PartialEq, Clone, Debug)]
#[repr(u8)]
pub enum NEXMarkQuery {
    CurrencyConversion = 1,
    // TODO: add more..
}

// Default Values

fn num_events() -> u32 {
    10000
}

fn num_events_generators() -> u32 {
    100
}

fn hot_action_ratio() -> u32 {
    2
}

fn hot_seller_ratio() -> u32 {
    4
}

fn hot_bidder_ratio() -> u32 {
    4
}

fn window_size_sec() -> u32 {
    10
}

fn window_period_sec() -> u32 {
    10
}

fn num_active_people() -> u32 {
    1000
}

fn in_flight_auctions() -> u32 {
    100
}

fn stream_timeout() -> u32 {
    100
}

fn person_id_lead() -> u32 {
    10
}

fn rate_shape() -> RateShape {
    RateShape::Sine
}

fn num_categories() -> u32 {
    5
}

fn out_of_order_group_size() -> u32 {
    1
}

fn us_states() -> Vec<String> {
    vec!["A".into()]
}

fn us_cities() -> Vec<String> {
    vec!["A".into()]
}

fn first_names() -> Vec<String> {
    Vec::new()
}

fn last_names() -> Vec<String> {
    Vec::new()
}
