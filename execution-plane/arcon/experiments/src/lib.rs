// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

extern crate arcon;
#[macro_use]
extern crate anyhow;

pub mod nexmark;
pub mod throughput_sink;

use arcon::{macros::*, prelude::*};

#[arcon_keyed(id)]
pub struct Item {
    #[prost(int32, tag = "1")]
    pub id: i32,
    #[prost(uint64, tag = "2")]
    pub number: u64,
    #[prost(uint64, tag = "3")]
    pub scaling_factor: u64,
}

#[arcon]
pub struct EnrichedItem {
    #[prost(int32, tag = "1")]
    pub id: i32,
    #[prost(message, tag = "2")]
    pub root: Option<ArconF64>,
}

// Workload function
#[inline(always)]
pub fn square_root_newton(square: u64, iters: usize) -> ArconF64 {
    let target = square as f64;
    let mut current_guess = target;
    for _ in 0..iters {
        let numerator = current_guess * current_guess - target;
        let denom = current_guess * 2.0;
        current_guess = current_guess - (numerator / denom);
    }

    current_guess.into()
}

// item generator
pub fn get_items(scaling_factor: u64, total_items: usize) -> Vec<Item> {
    let mut items = Vec::with_capacity(total_items as usize);
    for i in 0..total_items {
        items.push(Item {
            id: i as i32,
            number: i as u64 + 10,
            scaling_factor,
        });
    }
    items
}
