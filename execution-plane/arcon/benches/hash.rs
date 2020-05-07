// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

extern crate arcon;

use crate::arcon::ArconType;
use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

#[cfg_attr(feature = "arcon_serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(arcon::Arcon, prost::Message, Clone, abomonation_derive::Abomonation)]
#[arcon(unsafe_ser_id = 104, reliable_ser_id = 105, version = 1, keys = "id")]
pub struct SmallKey {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(uint32, tag = "2")]
    pub price: u32,
}

impl SmallKey {
    pub fn new() -> SmallKey {
        SmallKey { id: 10, price: 10 }
    }
}

impl Hash for SmallKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

#[cfg_attr(feature = "arcon_serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(arcon::Arcon, prost::Message, Clone, abomonation_derive::Abomonation)]
#[arcon(
    unsafe_ser_id = 104,
    reliable_ser_id = 105,
    version = 1,
    keys = "id, date,name,some_bytes,raw_msg,timestamp"
)]
pub struct LargerKey {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(string, tag = "2")]
    pub date: String,
    #[prost(string, tag = "3")]
    pub name: String,
    #[prost(bytes, tag = "4")]
    pub some_bytes: Vec<u8>,
    #[prost(bytes, tag = "5")]
    pub raw_msg: Vec<u8>,
    #[prost(uint64, tag = "6")]
    pub timestamp: u64,
}
impl LargerKey {
    pub fn new() -> LargerKey {
        LargerKey {
            id: 10,
            date: String::from("2020-02-10"),
            name: String::from("Andersson"),
            some_bytes: vec![0, 1, 0, 0, 1, 0, 0, 1, 0, 0, 1, 0, 1],
            raw_msg: vec![0, 1, 0, 0, 1, 0, 0, 1],
            timestamp: 12312839213821,
        }
    }
}

impl Hash for LargerKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.date.hash(state);
        self.name.hash(state);
        self.some_bytes.hash(state);
        self.raw_msg.hash(state);
        self.timestamp.hash(state);
    }
}

fn arcon_hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("arcon_hash");
    group.bench_function("arcon_small_key", arcon_small_key);
    group.bench_function("DefaultHasher small key", rust_default_small_key);

    group.bench_function("arcon_larger_key", arcon_larger_key);
    group.bench_function("DefaultHasher larger key", rust_default_larger_key);

    group.finish()
}

fn arcon_small_key(b: &mut Bencher) {
    let basic = SmallKey::new();
    b.iter(|| basic.get_key());
}
fn rust_default_small_key(b: &mut Bencher) {
    let basic = SmallKey::new();
    b.iter(|| {
        let mut state = DefaultHasher::new();
        basic.hash(&mut state);
        state.finish()
    });
}

fn arcon_larger_key(b: &mut Bencher) {
    let large = LargerKey::new();
    b.iter(|| large.get_key());
}

fn rust_default_larger_key(b: &mut Bencher) {
    let large = LargerKey::new();
    b.iter(|| {
        let mut state = DefaultHasher::new();
        large.hash(&mut state);
        state.finish()
    });
}

criterion_group!(benches, arcon_hash);
criterion_main!(benches);
