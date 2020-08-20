// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use arcon_state::{
    index::{hash::HashIndex, IndexOps},
    serialization::protobuf::serialize,
    *,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use itertools::Itertools;
use once_cell::sync::Lazy;
use rand::Rng;
use std::rc::Rc;
use tempfile::tempdir;

const MOD_FACTORS: [f32; 2] = [0.3, 0.8];
const CAPACITY: [usize; 2] = [5048, 32768]; // Capacity in amount of elements and not as in bytes size..
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

fn hash(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash");
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

    for input in MOD_FACTORS.iter().cartesian_product(CAPACITY.iter()) {
        let (mod_factor, capacity) = input;
        let description = format!("mod_factor: {}, capacity: {}", mod_factor, capacity);

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new(
                "SmallStruct Hot Keys Read Rocks Backed",
                description.clone(),
            ),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                read!(
                    HOT_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Rocks,
                    capacity,
                    mod_factor
                )
            },
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new(
                "SmallStruct Uniform Keys Read Rocks Backed",
                description.clone(),
            ),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                read!(
                    UNIFORM_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Rocks,
                    capacity,
                    mod_factor
                )
            },
        );

        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new("SmallStruct Hot Keys Read Sled Backed", description.clone()),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                read!(
                    HOT_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Sled,
                    capacity,
                    mod_factor
                )
            },
        );

        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new(
                "SmallStruct Uniform Keys Read Sled Backed",
                description.clone(),
            ),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                read!(
                    UNIFORM_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Sled,
                    capacity,
                    mod_factor
                )
            },
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new(
                "LargeStruct Hot Keys Read Rocks Backed",
                description.clone(),
            ),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                read!(
                    HOT_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Rocks,
                    capacity,
                    mod_factor
                )
            },
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new(
                "LargeStruct Uniform Keys Read Rocks Backed",
                description.clone(),
            ),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                read!(
                    UNIFORM_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Rocks,
                    capacity,
                    mod_factor
                )
            },
        );

        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new("LargeStruct Hot Keys Read Sled Backed", description.clone()),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                read!(
                    HOT_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Sled,
                    capacity,
                    mod_factor
                )
            },
        );

        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new(
                "LargeStruct Uniform Keys Read Sled Backed",
                description.clone(),
            ),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                read!(
                    UNIFORM_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Sled,
                    capacity,
                    mod_factor
                )
            },
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new(
                "SmallStruct Insert Hot Keys Rocks Backed",
                description.clone(),
            ),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                insert!(
                    HOT_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Rocks,
                    capacity,
                    mod_factor,
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
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                insert!(
                    HOT_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Rocks,
                    capacity,
                    mod_factor,
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
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                insert!(
                    UNIFORM_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Rocks,
                    capacity,
                    mod_factor,
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
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                insert!(
                    UNIFORM_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Rocks,
                    capacity,
                    mod_factor,
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
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                insert!(
                    HOT_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Sled,
                    capacity,
                    mod_factor,
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
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                insert!(
                    HOT_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Sled,
                    capacity,
                    mod_factor,
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
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                insert!(
                    UNIFORM_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Sled,
                    capacity,
                    mod_factor,
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
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                insert!(
                    UNIFORM_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Sled,
                    capacity,
                    mod_factor,
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
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                insert!(
                    HOT_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Rocks,
                    capacity,
                    mod_factor,
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
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                insert!(
                    HOT_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Rocks,
                    capacity,
                    mod_factor,
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
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                insert!(
                    UNIFORM_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Rocks,
                    capacity,
                    mod_factor,
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
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                insert!(
                    UNIFORM_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Rocks,
                    capacity,
                    mod_factor,
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
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                insert!(
                    HOT_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Sled,
                    capacity,
                    mod_factor,
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
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                insert!(
                    HOT_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Sled,
                    capacity,
                    mod_factor,
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
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                insert!(
                    UNIFORM_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Sled,
                    capacity,
                    mod_factor,
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
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                insert!(
                    UNIFORM_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Sled,
                    capacity,
                    mod_factor,
                    true
                )
            },
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new("RMW SmallStruct Hot Keys Rocks Backed", description.clone()),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                rmw!(
                    HOT_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Rocks,
                    capacity,
                    mod_factor,
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
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                rmw!(
                    UNIFORM_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Rocks,
                    capacity,
                    mod_factor,
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
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                rmw!(
                    HOT_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Rocks,
                    capacity,
                    mod_factor,
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
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                rmw!(
                    UNIFORM_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Rocks,
                    capacity,
                    mod_factor,
                    true
                )
            },
        );

        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new("RMW SmallStruct Hot Keys Sled Backed", description.clone()),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                rmw!(
                    HOT_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Sled,
                    capacity,
                    mod_factor,
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
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                rmw!(
                    UNIFORM_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Sled,
                    capacity,
                    mod_factor,
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
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                rmw!(
                    HOT_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Sled,
                    capacity,
                    mod_factor,
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
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                rmw!(
                    UNIFORM_KEYS,
                    b,
                    SmallStruct,
                    BackendType::Sled,
                    capacity,
                    mod_factor,
                    true
                )
            },
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new("RMW LargeStruct Hot Keys Rocks Backed", description.clone()),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                rmw!(
                    HOT_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Rocks,
                    capacity,
                    mod_factor,
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
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                rmw!(
                    HOT_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Rocks,
                    capacity,
                    mod_factor,
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
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                rmw!(
                    UNIFORM_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Rocks,
                    capacity,
                    mod_factor,
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
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                rmw!(
                    UNIFORM_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Rocks,
                    capacity,
                    mod_factor,
                    true
                )
            },
        );

        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new("RMW LargeStruct Hot Keys Sled Backed", description.clone()),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                rmw!(
                    HOT_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Sled,
                    capacity,
                    mod_factor,
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
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                rmw!(
                    HOT_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Sled,
                    capacity,
                    mod_factor,
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
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                rmw!(
                    UNIFORM_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Sled,
                    capacity,
                    mod_factor,
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
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                rmw!(
                    UNIFORM_KEYS,
                    b,
                    LargeStruct,
                    BackendType::Sled,
                    capacity,
                    mod_factor,
                    true
                )
            },
        );
    }

    // Finished with the HashIndex benches
    // Now onto pure backend stuff..

    let unused_param = 0;
    #[cfg(feature = "rocks")]
    group.bench_with_input(
        BenchmarkId::new("RMW SmallStruct Uniform Keys Pure Rocks", ""),
        &unused_param,
        |b, &_| {
            rmw_pure_backend!(UNIFORM_KEYS, b, SmallStruct, BackendType::Rocks);
        },
    );
    #[cfg(feature = "rocks")]
    group.bench_with_input(
        BenchmarkId::new("RMW SmallStruct Hot Keys Pure Rocks", ""),
        &unused_param,
        |b, &_| {
            rmw_pure_backend!(HOT_KEYS, b, SmallStruct, BackendType::Rocks);
        },
    );
    #[cfg(feature = "rocks")]
    group.bench_with_input(
        BenchmarkId::new("RMW LargeStruct Uniform Keys Pure Rocks", ""),
        &unused_param,
        |b, &_| {
            rmw_pure_backend!(UNIFORM_KEYS, b, LargeStruct, BackendType::Rocks);
        },
    );
    #[cfg(feature = "rocks")]
    group.bench_with_input(
        BenchmarkId::new("RMW LargeStruct Hot Keys Pure Rocks", ""),
        &unused_param,
        |b, &_| {
            rmw_pure_backend!(HOT_KEYS, b, LargeStruct, BackendType::Rocks);
        },
    );

    #[cfg(feature = "rocks")]
    group.bench_with_input(
        BenchmarkId::new("Read SmallStruct Uniform Keys Pure Rocks", ""),
        &unused_param,
        |b, &_| {
            read_pure_backend!(UNIFORM_KEYS, b, SmallStruct, BackendType::Rocks);
        },
    );
    #[cfg(feature = "rocks")]
    group.bench_with_input(
        BenchmarkId::new("Read SmallStruct Hot Keys Pure Rocks", ""),
        &unused_param,
        |b, &_| {
            read_pure_backend!(HOT_KEYS, b, SmallStruct, BackendType::Rocks);
        },
    );

    #[cfg(feature = "rocks")]
    group.bench_with_input(
        BenchmarkId::new("Read LargeStruct Uniform Keys Pure Rocks", ""),
        &unused_param,
        |b, &_| {
            read_pure_backend!(UNIFORM_KEYS, b, LargeStruct, BackendType::Rocks);
        },
    );

    #[cfg(feature = "rocks")]
    group.bench_with_input(
        BenchmarkId::new("Read LargeStruct Hot Keys Pure Rocks", ""),
        &unused_param,
        |b, &_| {
            read_pure_backend!(HOT_KEYS, b, LargeStruct, BackendType::Rocks);
        },
    );

    #[cfg(feature = "sled")]
    group.bench_with_input(
        BenchmarkId::new("Read SmallStruct Uniform Keys Pure Sled", ""),
        &unused_param,
        |b, &_| {
            read_pure_backend!(UNIFORM_KEYS, b, SmallStruct, BackendType::Sled);
        },
    );
    #[cfg(feature = "sled")]
    group.bench_with_input(
        BenchmarkId::new("Read SmallStruct Hot Keys Pure Sled", ""),
        &unused_param,
        |b, &_| {
            read_pure_backend!(HOT_KEYS, b, SmallStruct, BackendType::Sled);
        },
    );

    #[cfg(feature = "sled")]
    group.bench_with_input(
        BenchmarkId::new("Read LargeStruct Uniform Keys Pure Sled", ""),
        &unused_param,
        |b, &_| {
            read_pure_backend!(UNIFORM_KEYS, b, LargeStruct, BackendType::Sled);
        },
    );

    #[cfg(feature = "sled")]
    group.bench_with_input(
        BenchmarkId::new("Read LargeStruct Hot Keys Pure Sled", ""),
        &unused_param,
        |b, &_| {
            read_pure_backend!(HOT_KEYS, b, LargeStruct, BackendType::Sled);
        },
    );

    group.finish()
}

#[macro_export]
macro_rules! read {
    ($keys: expr, $bencher: expr, $type_value:ident, $backend:expr, $capacity:expr, $mod_factor:expr) => {{
        let dir = tempdir().unwrap();
        with_backend_type!($backend, |B| {
            let backend = B::create(dir.as_ref()).unwrap();
            let mut hash_index: HashIndex<u64, $type_value, B> =
                HashIndex::new("_hashindex", $capacity, $mod_factor, Rc::new(backend));

            for i in 0..TOTAL_KEYS {
                let _ = hash_index.put(i, $type_value::new()).unwrap();
            }

            $bencher.iter(|| {
                for i in $keys.iter() {
                    assert_eq!(hash_index.get(&i).unwrap().is_some(), true);
                }
            });
        });
    }};
}

#[macro_export]
macro_rules! insert {
    ($keys: expr, $bencher: expr, $type_value:ident, $backend:expr, $capacity:expr, $mod_factor:expr, $full_eviction:expr) => {{
        let dir = tempdir().unwrap();
        with_backend_type!($backend, |B| {
            let backend = B::create(dir.as_ref()).unwrap();
            let mut hash_index: HashIndex<u64, $type_value, B> =
                HashIndex::new("_hashindex", $capacity, $mod_factor, Rc::new(backend));

            if $full_eviction {
                $bencher.iter(|| {
                    for id in $keys.iter() {
                        let _ = hash_index.put(*id, $type_value::new()).unwrap();
                    }
                    let _ = hash_index.persist().unwrap();
                });
            } else {
                $bencher.iter(|| {
                    for id in $keys.iter() {
                        let _ = hash_index.put(*id, $type_value::new()).unwrap();
                    }
                });
            }
        });
    }};
}

#[macro_export]
macro_rules! rmw {
    ($keys: expr, $bencher: expr, $type_value:ident, $backend:expr, $capacity:expr, $mod_factor:expr, $full_eviction:expr) => {{
        let dir = tempdir().unwrap();
        with_backend_type!($backend, |B| {
            let backend = B::create(dir.as_ref()).unwrap();
            let mut hash_index: HashIndex<u64, $type_value, B> =
                HashIndex::new("_hashindex", $capacity, $mod_factor, Rc::new(backend));

            for i in 0..TOTAL_KEYS {
                let _ = hash_index.put(i, $type_value::new()).unwrap();
            }
            let _ = hash_index.persist().unwrap();

            if $full_eviction {
                $bencher.iter(|| {
                    for i in $keys.iter() {
                        hash_index
                            .rmw(&i, |val| {
                                val.x2 += 10;
                            })
                            .unwrap();
                    }
                    hash_index.persist().unwrap()
                });
            } else {
                $bencher.iter(|| {
                    for i in $keys.iter() {
                        hash_index
                            .rmw(&i, |val| {
                                val.x2 += 10;
                            })
                            .unwrap();
                    }
                });
            }
        });
    }};
}

#[macro_export]
macro_rules! rmw_pure_backend {
    ($keys: expr, $bencher: expr, $type_value:ident, $backend:expr) => {{
        let dir = tempdir().unwrap();
        with_backend_type!($backend, |B| {
            let backend = B::create(dir.as_ref()).unwrap();
            let mut map_handle: Handle<MapState<u64, $type_value>> = Handle::map("mapindex");
            let mut session = backend.session();
            {
                let mut rtok = unsafe { RegistrationToken::new(&mut session) };
                map_handle.register(&mut rtok);
            }

            let mut state = map_handle.activate(&mut session);
            // Fill in some keys
            for i in 0..TOTAL_KEYS {
                let _ = state.fast_insert(i, $type_value::new());
            }

            $bencher.iter(|| {
                for i in $keys.iter() {
                    let mut s = state.get(&i).unwrap().unwrap();
                    s.x2 += 10;
                    state.fast_insert(*i, s).unwrap()
                }
            });
        });
    }};
}

#[macro_export]
macro_rules! read_pure_backend {
    ($keys: expr, $bencher: expr, $type_value:ident, $backend:expr) => {{
        let dir = tempdir().unwrap();
        with_backend_type!($backend, |B| {
            let backend = B::create(dir.as_ref()).unwrap();
            let mut map_handle: Handle<MapState<u64, $type_value>> = Handle::map("mapindex");
            let mut session = backend.session();
            {
                let mut rtok = unsafe { RegistrationToken::new(&mut session) };
                map_handle.register(&mut rtok);
            }

            let mut state = map_handle.activate(&mut session);
            // Fill in some keys
            for i in 0..TOTAL_KEYS {
                let _ = state.fast_insert(i, $type_value::new());
            }

            $bencher.iter(|| {
                for i in $keys.iter() {
                    assert_eq!(state.get(&i).unwrap().is_some(), true);
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
    targets = hash,
}

criterion_main!(benches);
