// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

// Benchmarks for serialisation/deserialisation

use arcon::macros::*;
use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};
use kompact::prelude::IntoBuf;
use lz4_compression::prelude::{compress, decompress};
use prost::Message;
use serde::{Deserialize, Serialize};

#[derive(prost::Message, PartialEq, Serialize, Deserialize, Abomonation)]
pub struct SmallStruct {
    #[prost(int64, tag = "1")]
    pub x1: i64,
    #[prost(uint32, tag = "2")]
    pub x2: u32,
    #[prost(double, tag = "3")]
    pub x3: f64,
}

impl SmallStruct {
    pub fn new() -> SmallStruct {
        SmallStruct {
            x1: 100,
            x2: 500,
            x3: 1000.0,
        }
    }
}

#[derive(prost::Message, PartialEq, Serialize, Deserialize, Abomonation)]
pub struct LargeStruct {
    #[prost(int64, tag = "1")]
    pub x1: i64,
    #[prost(uint32, tag = "2")]
    pub x2: u32,
    #[prost(double, tag = "3")]
    pub x3: f64,
    #[prost(int64, repeated, tag = "4")]
    pub x4: Vec<i64>,
    #[prost(uint64, repeated, tag = "5")]
    pub x5: Vec<u64>,
    #[prost(double, repeated, tag = "6")]
    pub x6: Vec<f64>,
}

impl LargeStruct {
    pub fn new() -> LargeStruct {
        LargeStruct {
            x1: 50,
            x2: 1000,
            x3: 500.0,
            x4: vec![200, 300, 1000, 5000, 200, 350, 100],
            x5: vec![20, 50, 100, 20, 40, 100, 900, 100],
            x6: vec![150.0, 500.1, 35.5, 20.5, 40.9, 80.5, 400.5, 350.0],
        }
    }
}

fn arcon_serde_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("arcon_serde_bench");
    group.bench_function("small protobuf serialisation", protobuf_ser_small_struct);
    group.bench_function("small bincode serialisation", bincode_ser_small_struct);
    group.bench_function(
        "small abomonation serialisation",
        abomonation_ser_small_struct,
    );

    group.bench_function("large protobuf serialisation", protobuf_ser_large_struct);
    group.bench_function("large bincode serialisation", bincode_ser_large_struct);
    group.bench_function(
        "large abomonation serialisation",
        abomonation_ser_large_struct,
    );

    group.bench_function(
        "small protobuf deserialisation",
        protobuf_deser_small_struct,
    );
    group.bench_function("small bincode deserialisation", bincode_deser_small_struct);
    group.bench_function(
        "small abomonation deserialisation",
        abomonation_deser_small_struct,
    );

    group.bench_function(
        "large protobuf deserialisation",
        protobuf_deser_large_struct,
    );
    group.bench_function("large bincode deserialisation", bincode_deser_large_struct);
    group.bench_function(
        "large abomonation deserialisation",
        abomonation_deser_large_struct,
    );

    group.bench_function("full abomonation serde", abomonation_full_serde);

    group.bench_function(
        "full abomonation serde with lz4",
        abomonation_full_serde_lz4,
    );

    group.finish()
}

pub fn protobuf_ser_small_struct(b: &mut Bencher) {
    let small = SmallStruct::new();
    b.iter(|| protobuf_serialise(black_box(&small)));
}

pub fn protobuf_ser_large_struct(b: &mut Bencher) {
    let large = LargeStruct::new();
    b.iter(|| protobuf_serialise(black_box(&large)));
}
pub fn bincode_ser_small_struct(b: &mut Bencher) {
    let small = SmallStruct::new();
    b.iter(|| bincode_serialise(black_box(&small)));
}
pub fn bincode_ser_large_struct(b: &mut Bencher) {
    let large = LargeStruct::new();
    b.iter(|| bincode_serialise(black_box(&large)));
}
pub fn abomonation_ser_small_struct(b: &mut Bencher) {
    let small = SmallStruct::new();
    b.iter(|| abomonation_serialise(black_box(&small)));
}
pub fn abomonation_ser_large_struct(b: &mut Bencher) {
    let large = LargeStruct::new();
    b.iter(|| abomonation_serialise(black_box(&large)));
}

pub fn protobuf_serialise<A: prost::Message>(data: &A) {
    let mut bytes = Vec::with_capacity(data.encoded_len());
    data.encode(&mut bytes).unwrap();
    black_box(&bytes);
}

pub fn bincode_serialise<A: serde::Serialize>(data: &A) {
    let bytes = bincode::serialize(data).unwrap();
    black_box(&bytes);
}
pub fn abomonation_serialise<A: abomonation::Abomonation>(data: &A) {
    let mut buf = Vec::with_capacity(abomonation::measure(data));
    let _ = unsafe { abomonation::encode(data, &mut buf).unwrap() };
    black_box(&buf);
}

pub fn protobuf_deser_small_struct(b: &mut Bencher) {
    let small = SmallStruct::new();
    let mut bytes: Vec<u8> = Vec::with_capacity(small.encoded_len());
    small.encode(&mut bytes).unwrap();
    let buf = bytes.into_buf();
    b.iter(|| {
        // prost consumes the whole buffer, hence the clone....
        assert_eq!(&SmallStruct::decode(&mut buf.clone()).unwrap(), &small);
    });
}

pub fn protobuf_deser_large_struct(b: &mut Bencher) {
    let large = LargeStruct::new();
    let mut bytes = Vec::with_capacity(large.encoded_len());
    large.encode(&mut bytes).unwrap();
    let buf = bytes.into_buf();
    b.iter(|| {
        // prost consumes the whole buffer, hence the clone....
        assert_eq!(&LargeStruct::decode(&mut buf.clone()).unwrap(), &large);
    });
}

pub fn bincode_deser_small_struct(b: &mut Bencher) {
    let small = SmallStruct::new();
    let mut bytes = bincode::serialize(&small).unwrap();
    b.iter(|| {
        assert_eq!(
            &bincode::deserialize::<SmallStruct>(&mut bytes).unwrap(),
            &small
        );
    });
}

pub fn bincode_deser_large_struct(b: &mut Bencher) {
    let large = LargeStruct::new();
    let mut bytes = bincode::serialize(&large).unwrap();
    b.iter(|| {
        assert_eq!(
            &bincode::deserialize::<LargeStruct>(&mut bytes).unwrap(),
            &large
        );
    });
}

pub fn abomonation_deser_small_struct(b: &mut Bencher) {
    let small = SmallStruct::new();
    let mut bytes = Vec::with_capacity(abomonation::measure(&small));
    let _ = unsafe { abomonation::encode(&small, &mut bytes).unwrap() };
    b.iter(|| {
        assert_eq!(
            unsafe { abomonation::decode::<SmallStruct>(&mut bytes) }
                .unwrap()
                .0,
            &small
        );
    });
}

pub fn abomonation_deser_large_struct(b: &mut Bencher) {
    let large = LargeStruct::new();
    let mut bytes = Vec::with_capacity(abomonation::measure(&large));
    let _ = unsafe { abomonation::encode(&large, &mut bytes).unwrap() };
    b.iter(|| {
        assert_eq!(
            unsafe { abomonation::decode::<LargeStruct>(&mut bytes) }
                .unwrap()
                .0,
            &large
        );
    });
}

pub fn abomonation_full_serde(b: &mut Bencher) {
    let large = LargeStruct::new();
    b.iter(|| {
        let mut buf = Vec::with_capacity(abomonation::measure(&large));
        let _ = unsafe { abomonation::encode(&large, &mut buf).unwrap() };
        assert_eq!(
            unsafe { abomonation::decode::<LargeStruct>(&mut buf) }
                .unwrap()
                .0,
            &large
        );
    });
}

pub fn abomonation_full_serde_lz4(b: &mut Bencher) {
    let large = LargeStruct::new();
    b.iter(|| {
        let mut buf = Vec::with_capacity(abomonation::measure(&large));
        let _ = unsafe { abomonation::encode(&large, &mut buf).unwrap() };
        let compressed_data = compress(&buf);
        let mut uncompressed_data = decompress(&compressed_data).unwrap();
        assert_eq!(
            unsafe { abomonation::decode::<LargeStruct>(&mut uncompressed_data) }
                .unwrap()
                .0,
            &large
        );
    });
}

criterion_group!(benches, arcon_serde_bench);
criterion_main!(benches);
