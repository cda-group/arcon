// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use arcon_state::{
    index::{hash::HashIndex, IndexOps},
    *,
};
use criterion::{criterion_group, criterion_main, Bencher, BenchmarkId, Criterion, Throughput};
use itertools::Itertools;
use once_cell::sync::Lazy;
use rand::Rng;
use std::rc::Rc;
use tempfile::tempdir;

const MOD_FACTORS: [f32; 2] = [0.5, 0.8];
const CAPACITY: [usize; 2] = [4096, 10024];
const TOTAL_KEYS: u64 = 10000;
const TOTAL_OPERATIONS: u64 = 100000;

static RANDOM_INDEXES: Lazy<Vec<u64>> = Lazy::new(|| {
    let mut rng = rand::thread_rng();
    let mut indexes = Vec::with_capacity(TOTAL_OPERATIONS as usize);
    for _i in 0..TOTAL_OPERATIONS {
        indexes.push(rng.gen_range(0, TOTAL_KEYS));
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

    for input in MOD_FACTORS.iter().cartesian_product(CAPACITY.iter()) {
        let (mod_factor, capacity) = input;
        let description = format!("mod_factor: {}, capacity: {}", mod_factor, capacity);

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new("SmallStruct Random Read Rocks Backed", description.clone()),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                random_read!(b, SmallStruct, BackendType::Rocks, capacity, mod_factor)
            },
        );
        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new("SmallStruct Random Read Sled Backed", description.clone()),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                random_read!(b, SmallStruct, BackendType::Sled, capacity, mod_factor)
            },
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new("LargeStruct Random Read Rocks Backed", description.clone()),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                random_read!(b, LargeStruct, BackendType::Rocks, capacity, mod_factor)
            },
        );

        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new("LargeStruct Random Read Sled Backed", description.clone()),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                random_read!(b, LargeStruct, BackendType::Sled, capacity, mod_factor)
            },
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new("SmallStruct Insert Rocks Backed", description.clone()),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                insert!(b, SmallStruct, BackendType::Rocks, capacity, mod_factor)
            },
        );
        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new("SmallStruct Insert Sled Backed", description.clone()),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                insert!(b, SmallStruct, BackendType::Sled, capacity, mod_factor)
            },
        );
        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new("LargeStruct Insert Rocks Backed", description.clone()),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                insert!(b, LargeStruct, BackendType::Rocks, capacity, mod_factor)
            },
        );
        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new("LargeStruct Insert Sled Backed", description.clone()),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                insert!(b, LargeStruct, BackendType::Sled, capacity, mod_factor)
            },
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new("RMW SmallStruct Rocks Backed", description.clone()),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                rmw!(b, SmallStruct, BackendType::Rocks, capacity, mod_factor)
            },
        );
        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new("RMW SmallStruct Sled Backed", description.clone()),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                rmw!(b, SmallStruct, BackendType::Sled, capacity, mod_factor)
            },
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new("RMW LargeStruct Rocks Backed", description.clone()),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                rmw!(b, LargeStruct, BackendType::Rocks, capacity, mod_factor)
            },
        );

        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new("RMW LargeStruct Sled Backed", description.clone()),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| {
                rmw!(b, LargeStruct, BackendType::Sled, capacity, mod_factor)
            },
        );
    }
    #[cfg(feature = "rocks")]
    group.bench_function("RMW SmallStruct Pure Rocks", rmw_small_pure_rocks);
    #[cfg(feature = "sled")]
    group.bench_function("RMW SmallStruct Pure Sled", rmw_small_pure_sled);
    #[cfg(feature = "rocks")]
    group.bench_function("RMW LargeStruct Pure Rocks", rmw_large_pure_rocks);
    #[cfg(feature = "sled")]
    group.bench_function("RMW LargeStruct Pure Sled", rmw_large_pure_sled);

    #[cfg(feature = "rocks")]
    group.bench_function("Random Read SmallStruct Pure Rocks", read_small_pure_rocks);
    #[cfg(feature = "sled")]
    group.bench_function("Random Read SmallStruct Pure Sled", read_small_pure_sled);
    #[cfg(feature = "rocks")]
    group.bench_function("Random Read LargeStruct Pure Rocks", read_large_pure_rocks);
    #[cfg(feature = "sled")]
    group.bench_function("Random Read LargeStruct Pure Sled", read_large_pure_sled);

    group.finish()
}

#[macro_export]
macro_rules! random_read {
    ($bencher: expr, $type_value:ident, $backend:expr, $capacity:expr, $mod_factor:expr) => {{
        let dir = tempdir().unwrap();
        with_backend_type!($backend, |B| {
            let backend = B::create(dir.as_ref()).unwrap();
            let mut hash_index: HashIndex<u64, $type_value, B> =
                HashIndex::new("_hashindex", $capacity, $mod_factor, Rc::new(backend));

            for i in 0..TOTAL_KEYS {
                let _ = hash_index.put(i, $type_value::new()).unwrap();
            }
            $bencher.iter(|| {
                for i in RANDOM_INDEXES.iter() {
                    assert_eq!(hash_index.get(&i).unwrap().is_some(), true);
                }
            });
        });
    }};
}

#[macro_export]
macro_rules! insert {
    ($bencher: expr, $type_value:ident, $backend:expr, $capacity:expr, $mod_factor:expr) => {{
        let dir = tempdir().unwrap();
        with_backend_type!($backend, |B| {
            let backend = B::create(dir.as_ref()).unwrap();
            let mut hash_index: HashIndex<u64, $type_value, B> =
                HashIndex::new("_hashindex", $capacity, $mod_factor, Rc::new(backend));

            $bencher.iter(|| {
                for id in RANDOM_INDEXES.iter() {
                    let _ = hash_index.put(*id, $type_value::new()).unwrap();
                }
                let _ = hash_index.persist().unwrap();
            });
        });
    }};
}

#[macro_export]
macro_rules! rmw {
    ($bencher: expr, $type_value:ident, $backend:expr, $capacity:expr, $mod_factor:expr) => {{
        let dir = tempdir().unwrap();
        with_backend_type!($backend, |B| {
            let backend = B::create(dir.as_ref()).unwrap();
            let mut hash_index: HashIndex<u64, $type_value, B> =
                HashIndex::new("_hashindex", $capacity, $mod_factor, Rc::new(backend));

            for i in 0..TOTAL_KEYS {
                let _ = hash_index.put(i, $type_value::new()).unwrap();
            }

            $bencher.iter(|| {
                for i in RANDOM_INDEXES.iter() {
                    hash_index
                        .rmw(&i, |val| {
                            val.x2 += 10;
                        })
                        .unwrap();
                }
                hash_index.persist().unwrap()
            });
        });
    }};
}

#[cfg(feature = "rocks")]
fn rmw_small_pure_rocks(b: &mut Bencher) {
    rmw_pure_backend!(b, SmallStruct, BackendType::Rocks);
}
#[cfg(feature = "rocks")]
fn rmw_large_pure_rocks(b: &mut Bencher) {
    rmw_pure_backend!(b, LargeStruct, BackendType::Rocks);
}

#[cfg(feature = "sled")]
fn rmw_small_pure_sled(b: &mut Bencher) {
    rmw_pure_backend!(b, SmallStruct, BackendType::Sled);
}
#[cfg(feature = "sled")]
fn rmw_large_pure_sled(b: &mut Bencher) {
    rmw_pure_backend!(b, LargeStruct, BackendType::Sled);
}

#[macro_export]
macro_rules! rmw_pure_backend {
    ($bencher: expr, $type_value:ident, $backend:expr) => {{
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
                for i in RANDOM_INDEXES.iter() {
                    let mut s = state.get(&i).unwrap().unwrap();
                    s.x2 += 10;
                    state.fast_insert(*i, s).unwrap()
                }
            });
        });
    }};
}

#[cfg(feature = "rocks")]
fn read_small_pure_rocks(b: &mut Bencher) {
    read_pure_backend!(b, SmallStruct, BackendType::Rocks);
}
#[cfg(feature = "rocks")]
fn read_large_pure_rocks(b: &mut Bencher) {
    read_pure_backend!(b, LargeStruct, BackendType::Rocks);
}

#[cfg(feature = "sled")]
fn read_small_pure_sled(b: &mut Bencher) {
    read_pure_backend!(b, SmallStruct, BackendType::Sled);
}
#[cfg(feature = "sled")]
fn read_large_pure_sled(b: &mut Bencher) {
    read_pure_backend!(b, LargeStruct, BackendType::Sled);
}

#[macro_export]
macro_rules! read_pure_backend {
    ($bencher: expr, $type_value:ident, $backend:expr) => {{
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
                for i in RANDOM_INDEXES.iter() {
                    assert_eq!(state.get(&i).unwrap().is_some(), true);
                }
            });
        });
    }};
}

criterion_group!(benches, hash);
criterion_main!(benches);
