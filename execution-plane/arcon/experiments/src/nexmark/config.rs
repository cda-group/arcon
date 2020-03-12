// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use anyhow::{Context, Result};
use serde::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NexmarkConfig {
    pub query: usize,
    #[serde(default = "num_events")]
    pub num_events: usize,
    #[serde(default = "num_events_generators")]
    pub num_events_generators: usize,
    #[serde(default = "hot_action_ratio")]
    pub hot_action_ratio: usize,
    #[serde(default = "hot_sellers_ratio")]
    pub hot_sellers_ratio: usize,
    #[serde(default = "hot_bidders_ratio")]
    pub hot_bidders_ratio: usize,
    #[serde(default = "window_size_sec")]
    pub window_size_sec: usize,
    #[serde(default = "window_period_sec")]
    pub window_period_sec: usize,
    #[serde(default = "num_active_people")]
    pub num_active_people: usize,
    #[serde(default = "in_flight_auctions")]
    pub in_flight_auctions: usize,
}

impl NexmarkConfig {
    pub fn load(path: &str) -> Result<NexmarkConfig> {
        let data = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config from {}", path))?;
        toml::from_str(&data).context("Failed to parse toml")
    }
}

// DEFAULTS

fn num_events() -> usize {
    10000
}

fn num_events_generators() -> usize {
    100
}

fn hot_action_ratio() -> usize {
    2
}

fn hot_sellers_ratio() -> usize {
    4
}

fn hot_bidders_ratio() -> usize {
    4
}

fn window_size_sec() -> usize {
    10
}

fn window_period_sec() -> usize {
    10
}

fn num_active_people() -> usize {
    1000
}

fn in_flight_auctions() -> usize {
    100
}
