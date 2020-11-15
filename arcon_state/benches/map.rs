// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use arcon_state::{
    index::{map::Map, IndexOps},
    serialization::protobuf::serialize,
    EagerMap, *,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use once_cell::sync::Lazy;
use rand::Rng;
use std::sync::Arc;
use tempfile::tempdir;

const MOD_CAPACITY: [usize; 2] = [16384, 65536];
const READ_CAPACITY: [usize; 2] = [16384, 32768]; // Capacity in amount of elements and not as in bytes size..
const TOTAL_KEYS: u64 = 10000;
const TOTAL_OPERATIONS: u64 = 100000;

static UNIFORM_KEYS: Lazy<Vec<u64>> = Lazy::new(|| {
    let mut rng = rand::thread_rng();
    let mut indexes = Vec::with_capacity(TOTAL_OPERATIONS as usize);
    for _i in 0..TOTAL_OPERATIONS {
        indexes.push(rng.gen_range(0, TOTAL_KEYS));
    }
    indexes
});

static HOT_KEYS: Lazy<Vec<u64>> = Lazy::new(|| {
    let mut rng = rand::thread_rng();
    let mut indexes = Vec::with_capacity(TOTAL_OPERATIONS as usize);

    for _i in 0..(TOTAL_OPERATIONS / 4) {
        // generate two uniformly random keys
        let id = rng.gen_range(0, TOTAL_KEYS);
        indexes.push(id);
        let next_id = rng.gen_range(0, TOTAL_KEYS);
        indexes.push(next_id);

        // simulate hot id by pushing the ids in again
        indexes.push(id);
        indexes.push(next_id);
    }
    indexes
});

#[derive(prost::Message, Clone)]
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

#[derive(prost::Message, Clone)]
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

fn map(c: &mut Criterion) {
    let mut group = c.benchmark_group("map");
    group.throughput(Throughput::Elements(TOTAL_OPERATIONS));

    let small_bytes = serialize(&SmallStruct::new()).unwrap();
    let large_bytes = serialize(&LargeStruct::new()).unwrap();

    // Print some information regarding both serialised size and in-memory
    // of the structs that we are using.
    println!("SmallStruct Serialised Bytes Size {}", small_bytes.len());
    println!(
        "SmallStruct Mem Size {}",
        std::mem::size_of::<SmallStruct>()
    );
    println!("LargeStruct Serialised Bytes Size {}", large_bytes.len());
    println!(
        "LargeStruct Mem Size {}",
        std::mem::size_of::<LargeStruct>()
    );

    for input in MOD_CAPACITY.iter().zip(READ_CAPACITY.iter()) {
        let (mod_capacity, read_capacity) = input;
        let description = format!(
            "mod_capacity: {}, read_capacity: {}",
            mod_capacity, read_capacity
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new(
                "SmallStruct Hot Keys Read Rocks Backed",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                read!(
                    HOT_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Rocks,
                    mod_capacity,
                    read_capacity
                )
            },
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new(
                "SmallStruct Uniform Keys Read Rocks Backed",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                read!(
                    UNIFORM_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Rocks,
                    mod_capacity,
                    read_capacity
                )
            },
        );

        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new("SmallStruct Hot Keys Read Sled Backed", description.clone()),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                read!(
                    HOT_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Sled,
                    mod_capacity,
                    read_capacity
                )
            },
        );

        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new(
                "SmallStruct Uniform Keys Read Sled Backed",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                read!(
                    UNIFORM_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Sled,
                    mod_capacity,
                    read_capacity
                )
            },
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new(
                "LargeStruct Hot Keys Read Rocks Backed",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                read!(
                    HOT_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Rocks,
                    mod_capacity,
                    read_capacity
                )
            },
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new(
                "LargeStruct Uniform Keys Read Rocks Backed",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                read!(
                    UNIFORM_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Rocks,
                    mod_capacity,
                    read_capacity
                )
            },
        );

        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new("LargeStruct Hot Keys Read Sled Backed", description.clone()),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                read!(
                    HOT_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Sled,
                    mod_capacity,
                    read_capacity
                )
            },
        );

        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new(
                "LargeStruct Uniform Keys Read Sled Backed",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                read!(
                    UNIFORM_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Sled,
                    mod_capacity,
                    read_capacity
                )
            },
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new(
                "SmallStruct Insert Hot Keys Rocks Backed",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                insert!(
                    HOT_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Rocks,
                    mod_capacity,
                    read_capacity,
                    false
                )
            },
        );
        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new(
                "SmallStruct Insert Hot Keys Rocks Backed Full Eviction",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                insert!(
                    HOT_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Rocks,
                    mod_capacity,
                    read_capacity,
                    true
                )
            },
        );
        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new(
                "SmallStruct Insert Uniform Keys Rocks Backed",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                insert!(
                    UNIFORM_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Rocks,
                    mod_capacity,
                    read_capacity,
                    false
                )
            },
        );
        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new(
                "SmallStruct Insert Uniform Keys Rocks Backed Full Eviction",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                insert!(
                    UNIFORM_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Rocks,
                    mod_capacity,
                    read_capacity,
                    true
                )
            },
        );

        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new(
                "SmallStruct Insert Hot Keys Sled Backed",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                insert!(
                    HOT_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Sled,
                    mod_capacity,
                    read_capacity,
                    false
                )
            },
        );
        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new(
                "SmallStruct Insert Hot Keys Sled Backed Full Eviction",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                insert!(
                    HOT_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Sled,
                    mod_capacity,
                    read_capacity,
                    true
                )
            },
        );

        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new(
                "SmallStruct Insert Uniform Keys Sled Backed",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                insert!(
                    UNIFORM_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Sled,
                    mod_capacity,
                    read_capacity,
                    false
                )
            },
        );

        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new(
                "SmallStruct Insert Uniform Keys Sled Backed Full Eviction",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                insert!(
                    UNIFORM_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Sled,
                    mod_capacity,
                    read_capacity,
                    true
                )
            },
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new(
                "LargeStruct Insert Hot Keys Rocks Backed",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                insert!(
                    HOT_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Rocks,
                    mod_capacity,
                    read_capacity,
                    false
                )
            },
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new(
                "LargeStruct Insert Hot Keys Rocks Backed Full Eviction",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                insert!(
                    HOT_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Rocks,
                    mod_capacity,
                    read_capacity,
                    true
                )
            },
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new(
                "LargeStruct Insert Uniform Keys Rocks Backed",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                insert!(
                    UNIFORM_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Rocks,
                    mod_capacity,
                    read_capacity,
                    false
                )
            },
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new(
                "LargeStruct Insert Uniform Keys Rocks Backed Full Eviction",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                insert!(
                    UNIFORM_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Rocks,
                    mod_capacity,
                    read_capacity,
                    true
                )
            },
        );

        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new(
                "LargeStruct Insert Hot Keys Sled Backed",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                insert!(
                    HOT_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Sled,
                    mod_capacity,
                    read_capacity,
                    false
                )
            },
        );
        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new(
                "LargeStruct Insert Hot Keys Sled Backed Full Eviction",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                insert!(
                    HOT_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Sled,
                    mod_capacity,
                    read_capacity,
                    true
                )
            },
        );

        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new(
                "LargeStruct Insert Uniform Keys Sled Backed",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                insert!(
                    UNIFORM_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Sled,
                    mod_capacity,
                    read_capacity,
                    false
                )
            },
        );
        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new(
                "LargeStruct Insert Uniform Keys Sled Backed Full Eviction",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                insert!(
                    UNIFORM_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Sled,
                    mod_capacity,
                    read_capacity,
                    true
                )
            },
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new("RMW SmallStruct Hot Keys Rocks Backed", description.clone()),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                rmw!(
                    HOT_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Rocks,
                    mod_capacity,
                    read_capacity,
                    false
                )
            },
        );
        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new(
                "RMW SmallStruct Uniform Keys Rocks Backed",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                rmw!(
                    UNIFORM_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Rocks,
                    mod_capacity,
                    read_capacity,
                    false
                )
            },
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new(
                "RMW SmallStruct Hot Keys Rocks Backed Full Eviction",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                rmw!(
                    HOT_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Rocks,
                    mod_capacity,
                    read_capacity,
                    true
                )
            },
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new(
                "RMW SmallStruct Uniform Keys Rocks Backed Full Eviction",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                rmw!(
                    UNIFORM_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Rocks,
                    mod_capacity,
                    read_capacity,
                    true
                )
            },
        );

        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new("RMW SmallStruct Hot Keys Sled Backed", description.clone()),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                rmw!(
                    HOT_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Sled,
                    mod_capacity,
                    read_capacity,
                    false
                )
            },
        );

        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new(
                "RMW SmallStruct Uniform Keys Sled Backed",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                rmw!(
                    UNIFORM_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Sled,
                    mod_capacity,
                    read_capacity,
                    false
                )
            },
        );

        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new(
                "RMW SmallStruct Hot Keys Sled Backed Full Eviction",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                rmw!(
                    HOT_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Sled,
                    mod_capacity,
                    read_capacity,
                    true
                )
            },
        );

        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new(
                "RMW SmallStruct Uniform Keys Sled Backed Full Eviction",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                rmw!(
                    UNIFORM_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Sled,
                    mod_capacity,
                    read_capacity,
                    true
                )
            },
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new("RMW LargeStruct Hot Keys Rocks Backed", description.clone()),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                rmw!(
                    HOT_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Rocks,
                    mod_capacity,
                    read_capacity,
                    false
                )
            },
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new(
                "RMW LargeStruct Hot Keys Rocks Backed Full Eviction",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                rmw!(
                    HOT_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Rocks,
                    mod_capacity,
                    read_capacity,
                    true
                )
            },
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new(
                "RMW LargeStruct Uniform Keys Rocks Backed",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                rmw!(
                    UNIFORM_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Rocks,
                    mod_capacity,
                    read_capacity,
                    false
                )
            },
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new(
                "RMW LargeStruct Uniform Keys Rocks Backed Full Eviction",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                rmw!(
                    UNIFORM_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Rocks,
                    mod_capacity,
                    read_capacity,
                    true
                )
            },
        );

        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new("RMW LargeStruct Hot Keys Sled Backed", description.clone()),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                rmw!(
                    HOT_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Sled,
                    mod_capacity,
                    read_capacity,
                    false
                )
            },
        );

        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new(
                "RMW LargeStruct Hot Keys Sled Backed Full Eviction",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                rmw!(
                    HOT_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Sled,
                    mod_capacity,
                    read_capacity,
                    true
                )
            },
        );
        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new(
                "RMW LargeStruct Uniform Keys Sled Backed",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                rmw!(
                    UNIFORM_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Sled,
                    mod_capacity,
                    read_capacity,
                    false
                )
            },
        );
        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new(
                "RMW LargeStruct Uniform Keys Sled Backed Full Eviction",
                description.clone(),
            ),
            &(mod_capacity, read_capacity),
            |b, (&mod_capacity, &read_capacity)| {
                rmw!(
                    UNIFORM_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Sled,
                    mod_capacity,
                    read_capacity,
                    true
                )
            },
        );
    }

    // Finished with the Map benches
    // Now onto pure backend stuff..

    let unused_param = 0;
    #[cfg(feature = "rocks")]
    group.bench_with_input(
        BenchmarkId::new("RMW SmallStruct Uniform Keys Eager Rocks", ""),
        &unused_param,
        |b, &_| {
            rmw_eager!(UNIFORM_KEYS, b, SmallStruct, BackendType::Rocks);
        },
    );
    #[cfg(feature = "rocks")]
    group.bench_with_input(
        BenchmarkId::new("RMW SmallStruct Hot Keys Eager Rocks", ""),
        &unused_param,
        |b, &_| {
            rmw_eager!(HOT_KEYS, b, SmallStruct, BackendType::Rocks);
        },
    );
    #[cfg(feature = "rocks")]
    group.bench_with_input(
        BenchmarkId::new("RMW LargeStruct Uniform Keys Eager Rocks", ""),
        &unused_param,
        |b, &_| {
            rmw_eager!(UNIFORM_KEYS, b, LargeStruct, BackendType::Rocks);
        },
    );
    #[cfg(feature = "rocks")]
    group.bench_with_input(
        BenchmarkId::new("RMW LargeStruct Hot Keys Eager Rocks", ""),
        &unused_param,
        |b, &_| {
            rmw_eager!(HOT_KEYS, b, LargeStruct, BackendType::Rocks);
        },
    );

    #[cfg(feature = "rocks")]
    group.bench_with_input(
        BenchmarkId::new("Read SmallStruct Uniform Keys Eager Rocks", ""),
        &unused_param,
        |b, &_| {
            read_eager!(UNIFORM_KEYS, b, SmallStruct, BackendType::Rocks);
        },
    );
    #[cfg(feature = "rocks")]
    group.bench_with_input(
        BenchmarkId::new("Read SmallStruct Hot Keys Eager Rocks", ""),
        &unused_param,
        |b, &_| {
            read_eager!(HOT_KEYS, b, SmallStruct, BackendType::Rocks);
        },
    );

    #[cfg(feature = "rocks")]
    group.bench_with_input(
        BenchmarkId::new("Read LargeStruct Uniform Keys Eager Rocks", ""),
        &unused_param,
        |b, &_| {
            read_eager!(UNIFORM_KEYS, b, LargeStruct, BackendType::Rocks);
        },
    );

    #[cfg(feature = "rocks")]
    group.bench_with_input(
        BenchmarkId::new("Read LargeStruct Hot Keys Eager Rocks", ""),
        &unused_param,
        |b, &_| {
            read_eager!(HOT_KEYS, b, LargeStruct, BackendType::Rocks);
        },
    );

    #[cfg(feature = "sled")]
    group.bench_with_input(
        BenchmarkId::new("Read SmallStruct Uniform Keys Eager Sled", ""),
        &unused_param,
        |b, &_| {
            read_eager!(UNIFORM_KEYS, b, SmallStruct, BackendType::Sled);
        },
    );
    #[cfg(feature = "sled")]
    group.bench_with_input(
        BenchmarkId::new("Read SmallStruct Hot Keys Eager Sled", ""),
        &unused_param,
        |b, &_| {
            read_eager!(HOT_KEYS, b, SmallStruct, BackendType::Sled);
        },
    );

    #[cfg(feature = "sled")]
    group.bench_with_input(
        BenchmarkId::new("Read LargeStruct Uniform Keys Eager Sled", ""),
        &unused_param,
        |b, &_| {
            read_eager!(UNIFORM_KEYS, b, LargeStruct, BackendType::Sled);
        },
    );

    #[cfg(feature = "sled")]
    group.bench_with_input(
        BenchmarkId::new("Read LargeStruct Hot Keys Eager Sled", ""),
        &unused_param,
        |b, &_| {
            read_eager!(HOT_KEYS, b, LargeStruct, BackendType::Sled);
        },
    );

    group.finish()
}

#[macro_export]
macro_rules! read {
    ($keys: expr, $bencher: expr, $type_value:ident, $backend:expr, $mod_capacity:expr, $read_capacity:expr) => {{
        let dir = tempdir().unwrap();
        with_backend_type!($backend, |B| {
            let backend = Arc::new(B::create(dir.as_ref()).unwrap());
            let mut map_handle: Handle<MapState<u64, $type_value>> = Handle::map("mapindex");
            backend.register_map_handle(&mut map_handle);
            let state = map_handle.activate(backend.clone());
            let mut map: Map<u64, $type_value, B> =
                Map::with_capacity(state, $mod_capacity, $read_capacity);

            for i in 0..TOTAL_KEYS {
                let _ = map.put(i, $type_value::new()).unwrap();
            }

            $bencher.iter(|| {
                for i in $keys.iter() {
                    assert_eq!(map.get(&i).unwrap().is_some(), true, "Failed to get()");
                }
            });
        });
    }};
}

#[macro_export]
macro_rules! insert {
    ($keys: expr, $bencher: expr, $type_value:ident, $backend:expr, $mod_capacity:expr, $read_capacity:expr, $full_eviction:expr) => {{
        let dir = tempdir().unwrap();
        with_backend_type!($backend, |B| {
            let backend = Arc::new(B::create(dir.as_ref()).unwrap());
            let mut map_handle: Handle<MapState<u64, $type_value>> = Handle::map("mapindex");
            backend.register_map_handle(&mut map_handle);
            let state = map_handle.activate(backend.clone());
            let mut map: Map<u64, $type_value, B> =
                Map::with_capacity(state, $mod_capacity, $read_capacity);

            if $full_eviction {
                $bencher.iter(|| {
                    for id in $keys.iter() {
                        let _ = map.put(*id, $type_value::new()).unwrap();
                    }
                    let _ = map.persist().unwrap();
                });
            } else {
                $bencher.iter(|| {
                    for id in $keys.iter() {
                        let _ = map.put(*id, $type_value::new()).unwrap();
                    }
                });
            }
        });
    }};
}

#[macro_export]
macro_rules! rmw {
    ($keys: expr, $bencher: expr, $type_value:ident, $backend:expr, $mod_capacity:expr, $read_capacity:expr, $full_eviction:expr) => {{
        let dir = tempdir().unwrap();
        with_backend_type!($backend, |B| {
            let backend = Arc::new(B::create(dir.as_ref()).unwrap());
            let mut map_handle: Handle<MapState<u64, $type_value>> = Handle::map("mapindex");
            backend.register_map_handle(&mut map_handle);
            let state = map_handle.activate(backend.clone());

            let mut map: Map<u64, $type_value, B> =
                Map::with_capacity(state, $mod_capacity, $read_capacity);

            for i in 0..TOTAL_KEYS {
                let _ = map.put(i, $type_value::new()).unwrap();
            }
            let _ = map.persist().unwrap();

            if $full_eviction {
                $bencher.iter(|| {
                    for i in $keys.iter() {
                        map.rmw(
                            &i,
                            || $type_value::new(),
                            |val| {
                                val.x2 += 10;
                            },
                        )
                        .unwrap();
                    }
                    map.persist().unwrap()
                });
            } else {
                $bencher.iter(|| {
                    for i in $keys.iter() {
                        map.rmw(
                            &i,
                            || $type_value::new(),
                            |val| {
                                val.x2 += 10;
                            },
                        )
                        .unwrap();
                    }
                });
            }
        });
    }};
}

#[macro_export]
macro_rules! rmw_eager {
    ($keys: expr, $bencher: expr, $type_value:ident, $backend:expr) => {{
        let dir = tempdir().unwrap();
        with_backend_type!($backend, |B| {
            let backend = Arc::new(B::create(dir.as_ref()).unwrap());
            let mut map_handle: Handle<MapState<u64, $type_value>> = Handle::map("mapindex");
            backend.register_map_handle(&mut map_handle);
            let state = map_handle.activate(backend.clone());
            let mut eager_map = EagerMap::new(state);

            // Fill in some keys
            for i in 0..TOTAL_KEYS {
                let _ = eager_map.put(i, $type_value::new());
            }

            $bencher.iter(|| {
                for i in $keys.iter() {
                    let mut s = eager_map.get(&i).unwrap().unwrap();
                    s.x2 += 10;
                    eager_map.put(*i, s).unwrap()
                }
            });
        });
    }};
}

#[macro_export]
macro_rules! read_eager {
    ($keys: expr, $bencher: expr, $type_value:ident, $backend:expr) => {{
        let dir = tempdir().unwrap();
        with_backend_type!($backend, |B| {
            let backend = Arc::new(B::create(dir.as_ref()).unwrap());
            let mut map_handle: Handle<MapState<u64, $type_value>> = Handle::map("mapindex");
            backend.register_map_handle(&mut map_handle);
            let state = map_handle.activate(backend.clone());
            let mut eager_map = EagerMap::new(state);

            // Fill in some keys
            for i in 0..TOTAL_KEYS {
                let _ = eager_map.put(i, $type_value::new());
            }

            $bencher.iter(|| {
                for i in $keys.iter() {
                    assert_eq!(eager_map.get(&i).unwrap().is_some(), true);
                }
            });
        });
    }};
}

fn custom_criterion() -> Criterion {
    Criterion::default().sample_size(10)
}

criterion_group! {
    name = benches;
    config = custom_criterion();
    targets = map,
}

criterion_main!(benches);
