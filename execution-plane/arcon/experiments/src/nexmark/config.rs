// Copyright (c) 2017, 2018 ETH Zurich
// SPDX-License-Identifier: MIT
// Modifications Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use anyhow::{Context, Result};
use serde::*;
use serde_repr::*;
use std::{f64::consts::PI, path::Path};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RateShape {
    Square,
    Sine,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NEXMarkConfig {
    #[serde(default = "nexmark_query")]
    pub query: NEXMarkQuery,
    #[serde(default = "num_events")]
    pub num_events: u32,
    #[serde(default = "num_events_generators")]
    pub num_events_generators: u32,
    #[serde(default = "hot_auction_ratio")]
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
    pub stream_timeout: u64,
    #[serde(default = "person_id_lead")]
    pub person_id_lead: u32,
    #[serde(default = "rate_shape")]
    pub rate_shape: RateShape,
    #[serde(default = "num_categories")]
    pub num_categories: u32,
    #[serde(default = "first_rate")]
    pub first_rate: u32,
    #[serde(default = "next_rate")]
    pub next_rate: u32,
    #[serde(default = "out_of_order_group_size")]
    pub out_of_order_group_size: u32,
    #[serde(default = "time_dilation")]
    pub time_dilation: u64,
    // Filled during setup
    #[serde(default)]
    pub inter_event_delays_ns: Vec<f64>,
    // Filled during setup
    #[serde(default)]
    pub step_length: u32,
    #[serde(default)]
    // Filled during setup
    pub base_time_ns: u32,
    #[serde(default)]
    pub epoch_period: f32,
    #[serde(default = "first_event_id")]
    pub first_event_id: u32,
    #[serde(default = "first_event_number")]
    pub first_event_number: u32,
    #[serde(default = "auction_id_lead")]
    pub auction_id_lead: u32,
    #[serde(default = "hot_seller_ratio_2")]
    pub hot_seller_ratio_2: u32,
    #[serde(default = "hot_auction_ratio_2")]
    pub hot_auction_ratio_2: u32,
    #[serde(default = "hot_bidder_ratio_2")]
    pub hot_bidder_ratio_2: u32,
    #[serde(default = "person_proportion")]
    pub person_proportion: u32,
    #[serde(default = "auction_proportion")]
    pub auction_proportion: u32,
    #[serde(default = "bid_proportion")]
    pub bid_proportion: u32,
    #[serde(default)]
    pub proportion_denominator: u32,
    #[serde(default = "rate_period")]
    pub rate_period: u32,
    #[serde(default = "first_auction_id")]
    pub first_auction_id: u32,
    #[serde(default = "first_person_id")]
    pub first_person_id: u32,
    #[serde(default = "first_category_id")]
    pub first_category_id: u32,
    #[serde(default = "sine_approx_steps")]
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
    pub fn default() -> NEXMarkConfig {
        // NOTE: this will make sure it loads the defaults from serde...
        toml::from_str("")
            .map_err(|e| anyhow!("failed to parse conf {}", e))
            .unwrap()
    }

    pub fn load(path: impl AsRef<Path>) -> Result<NEXMarkConfig> {
        let path = path.as_ref();
        let data = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config from {}", path.display()))?;
        toml::from_str(&data).map_err(|e| anyhow!("failed to parse toml with err {}", e))
    }

    // TODO: fix hardcoded stuff..
    pub fn finish(conf: &mut NEXMarkConfig) {
        let first_rate = conf.first_rate;
        let next_rate = conf.next_rate;
        let mut inter_event_delays_ns = Vec::new();
        let ns_per_unit = 1_000_000_000;
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
                    for i in 0..conf.sine_approx_steps {
                        let r = (2.0 * PI * i as f64) / conf.sine_approx_steps as f64;
                        let rate = mid + amp * r.cos();
                        inter_event_delays_ns.push(
                            rate_to_period(rate.round() as u32) * conf.num_events_generators as f64,
                        );
                    }
                }
            }
        }

        conf.inter_event_delays_ns = inter_event_delays_ns;
        let n = if conf.rate_shape == RateShape::Square {
            2
        } else {
            conf.sine_approx_steps
        };
        let step_length = (conf.rate_period + n - 1) / n;
        //let events_per_epoch = 0;

        conf.step_length = step_length;
        conf.proportion_denominator =
            conf.person_proportion + conf.auction_proportion + conf.bid_proportion;

        if conf.inter_event_delays_ns.len() > 1 {
            panic!("non-constant rate not supported");
        }
    }

    #[inline]
    pub fn event_timestamp_ns(&self, event_number: u32) -> u32 {
        return self.base_time_ns + ((event_number as f64 * self.inter_event_delays_ns[0]) as u32);
    }

    #[inline]
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
    LocalItemSuggestion = 3,
    // TODO: add more..
}

impl NEXMarkQuery {
    pub fn new(i: u8) -> Result<Self> {
        match i {
            1 => Ok(NEXMarkQuery::CurrencyConversion),
            3 => Ok(NEXMarkQuery::LocalItemSuggestion),
            x => bail!("Wrong NEXMark query: {}", x),
        }
    }
}

// Default Values

fn nexmark_query() -> NEXMarkQuery {
    NEXMarkQuery::CurrencyConversion
}

fn num_events() -> u32 {
    100000000
}

fn num_events_generators() -> u32 {
    100
}

fn hot_auction_ratio() -> u32 {
    2
}
fn hot_auction_ratio_2() -> u32 {
    100
}

fn hot_seller_ratio() -> u32 {
    4
}

fn hot_seller_ratio_2() -> u32 {
    100
}

fn hot_bidder_ratio() -> u32 {
    4
}

fn hot_bidder_ratio_2() -> u32 {
    100
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

fn stream_timeout() -> u64 {
    100
}

fn person_id_lead() -> u32 {
    10
}

fn auction_id_lead() -> u32 {
    10
}

fn person_proportion() -> u32 {
    1
}

fn auction_proportion() -> u32 {
    3
}

fn bid_proportion() -> u32 {
    3
}

fn time_dilation() -> u64 {
    1
}

fn first_rate() -> u32 {
    1000
}

fn next_rate() -> u32 {
    1000
}

fn rate_shape() -> RateShape {
    RateShape::Sine
}

fn sine_approx_steps() -> u32 {
    10
}
fn first_event_id() -> u32 {
    0
}
fn first_event_number() -> u32 {
    0
}

fn rate_period() -> u32 {
    600
}

fn first_auction_id() -> u32 {
    1000
}
fn first_person_id() -> u32 {
    1000
}
fn first_category_id() -> u32 {
    10
}

fn num_categories() -> u32 {
    5
}

fn out_of_order_group_size() -> u32 {
    1
}

fn us_states() -> Vec<String> {
    vec![
        "AZ".into(),
        "CA".into(),
        "ID".into(),
        "OR".into(),
        "WA".into(),
        "WY".into(),
    ]
}

fn us_cities() -> Vec<String> {
    vec![
        "Phoenix".into(),
        "Los Angeles".into(),
        "Seattle".into(),
        "San Francisco".into(),
    ]
}

fn first_names() -> Vec<String> {
    vec![
        "Ben".into(),
        "Andrew".into(),
        "Steve".into(),
        "Derek".into(),
    ]
}

fn last_names() -> Vec<String> {
    vec![
        "Svensson".into(),
        "Andersson".into(),
        "Antonsson".into(),
        "Rikardsson".into(),
    ]
}

pub struct NexMarkInputTimes {
    config: NEXMarkConfig,
    next: Option<u64>,
    events_so_far: usize,
    end: u64,
    time_dilation: usize,
    peers: usize,
}

impl NexMarkInputTimes {
    pub fn new(config: NEXMarkConfig, end: u64, time_dilation: usize, peers: usize) -> Self {
        let mut this = Self {
            config,
            next: None,
            events_so_far: 0,
            end,
            time_dilation,
            peers,
        };
        this.make_next();
        this
    }

    fn make_next(&mut self) {
        let ts = self
            .config
            .event_timestamp_ns(self.config.next_adjusted_event(self.events_so_far as u32))
            as u64;
        let ts = ts / self.time_dilation as u64;
        if ts < self.end {
            self.events_so_far += self.peers;
            self.next = Some(ts);
        } else {
            self.next = None;
        };
    }
}

impl Iterator for NexMarkInputTimes {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        let s = self.next;
        self.make_next();
        s
    }
}
