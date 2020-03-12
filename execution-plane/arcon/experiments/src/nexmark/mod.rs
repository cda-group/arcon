// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

extern crate arcon;

pub mod config;

use arcon::{macros::*, prelude::*};


#[arcon]
pub struct Person {
    #[prost(uint32, tag = "1")]
    pub id: u32,
    #[prost(string, tag = "2")]
    pub name: String,
    #[prost(string, tag = "3")]
    pub email_address: String,
    #[prost(string, tag = "4")]
    pub credit_card: String,
    #[prost(string, tag = "5")]
    pub city: String,
    #[prost(string, tag = "6")]
    pub state: String,
    #[prost(uint32, tag = "7")]
    pub date: u32,
}

#[arcon]
pub struct Auction {
    #[prost(uint32, tag = "1")]
    pub id: u32,
    #[prost(string, tag = "2")]
    pub item_name: String,
    #[prost(string, tag = "3")]
    pub description: String,
    #[prost(uint32, tag = "4")]
    pub initial_bid: u32,
    #[prost(uint32, tag = "5")]
    pub reserve: u32,
    #[prost(uint32, tag = "6")]
    pub date: u32,
    #[prost(uint32, tag = "7")]
    pub expires: u32,
    #[prost(uint32, tag = "8")]
    pub seller: u32,
    #[prost(uint32, tag = "9")]
    pub category: u32,
}

#[arcon]
pub struct Bid {
    #[prost(uint32, tag = "1")]
    pub auction: u32,
    #[prost(uint32, tag = "2")]
    pub bidder: u32,
    #[prost(uint32, tag = "3")]
    pub price: u32,
    #[prost(uint32, tag = "4")]
    pub date: u32,
}

pub struct Event {}
