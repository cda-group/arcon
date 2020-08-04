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

const MOD_FACTORS: [f32; 3] = [0.3, 0.5, 0.8];
const CAPACITY: [usize; 3] = [512, 4096, 10024];
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
            BenchmarkId::new("SmallStruct Insert Rocks Backed", description.clone()),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| insert_small_rocks(b, capacity, mod_factor),
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new("SmallStruct Random Read Rocks Backed", description.clone()),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| read_small_rocks(b, capacity, mod_factor),
        );

        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new("SmallStruct Insert Sled Backed", description.clone()),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| insert_small_sled(b, capacity, mod_factor),
        );
        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new("SmallStruct Random Read Sled Backed", description.clone()),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| read_small_sled(b, capacity, mod_factor),
        );

        #[cfg(feature = "rocks")]
        group.bench_with_input(
            BenchmarkId::new("RMW SmallStruct Rocks Backed", description.clone()),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| rmw_small_rocks(b, capacity, mod_factor),
        );

        #[cfg(feature = "sled")]
        group.bench_with_input(
            BenchmarkId::new("RMW SmallStruct Sled Backed", description.clone()),
            &(mod_factor, capacity),
            |b, (&mod_factor, &capacity)| rmw_small_sled(b, capacity, mod_factor),
        );
    }

    #[cfg(feature = "rocks")]
    group.bench_function("RMW Pure Rocks SmallStruct", rmw_small_pure_rocks);
    #[cfg(feature = "sled")]
    group.bench_function("RMW Pure Sled SmallStruct", rmw_small_pure_sled);
    #[cfg(feature = "rocks")]
    group.bench_function("Random Read Pure Rocks SmallStruct", read_small_pure_rocks);
    #[cfg(feature = "sled")]
    group.bench_function("Random Read Pure Sled SmallStruct", read_small_pure_sled);

    group.finish()
}


#[cfg(feature = "rocks")]
fn read_small_rocks(b: &mut Bencher, capacity: usize, mod_factor: f32) {
    read_small(BackendType::Rocks, b, capacity, mod_factor);
}
#[cfg(feature = "sled")]
fn read_small_sled(b: &mut Bencher, capacity: usize, mod_factor: f32) {
    read_small(BackendType::Sled, b, capacity, mod_factor);
}

fn read_small(backend: BackendType, b: &mut Bencher, capacity: usize, mod_factor: f32) {
    let dir = tempdir().unwrap();
    with_backend_type!(backend, |B| {
        let backend = B::create(dir.as_ref()).unwrap();
        let mut hash_index: HashIndex<u64, SmallStruct, B> =
            HashIndex::new("_hashindex", capacity, mod_factor, Rc::new(backend));

        for i in 0..TOTAL_KEYS {
            hash_index.put(i, SmallStruct::new());
        }
        b.iter(|| {
            for i in RANDOM_INDEXES.iter() {
                assert_eq!(hash_index.get(&i).is_some(), true);
            }
        });
    });
}

#[cfg(feature = "rocks")]
fn insert_small_rocks(b: &mut Bencher, capacity: usize, mod_factor: f32) {
    insert_small(BackendType::Rocks, b, capacity, mod_factor);
}
#[cfg(feature = "sled")]
fn insert_small_sled(b: &mut Bencher, capacity: usize, mod_factor: f32) {
    insert_small(BackendType::Sled, b, capacity, mod_factor);
}

fn insert_small(backend: BackendType, b: &mut Bencher, capacity: usize, mod_factor: f32) {
    let dir = tempdir().unwrap();
    with_backend_type!(backend, |B| {
        let backend = B::create(dir.as_ref()).unwrap();
        let mut hash_index: HashIndex<u64, SmallStruct, B> =
            HashIndex::new("_hashindex", capacity, mod_factor, Rc::new(backend));

        b.iter(|| {
            for id in RANDOM_INDEXES.iter() {
                hash_index.put(*id, SmallStruct::new());
            }
            let _ = hash_index.persist().unwrap();
        });
    });
}

#[cfg(feature = "rocks")]
fn rmw_small_rocks(b: &mut Bencher, capacity: usize, mod_factor: f32) {
    rmw_small(BackendType::Rocks, b, capacity, mod_factor);
}
#[cfg(feature = "sled")]
fn rmw_small_sled(b: &mut Bencher, capacity: usize, mod_factor: f32) {
    rmw_small(BackendType::Sled, b, capacity, mod_factor);
}

fn rmw_small(backend: BackendType, b: &mut Bencher, capacity: usize, mod_factor: f32) {
    let dir = tempdir().unwrap();
    with_backend_type!(backend, |B| {
        let backend = B::create(dir.as_ref()).unwrap();
        let mut hash_index: HashIndex<u64, SmallStruct, B> =
            HashIndex::new("_hashindex", capacity, mod_factor, Rc::new(backend));
        // Fill in some keys
        for i in 0..TOTAL_KEYS {
            hash_index.put(i, SmallStruct::new());
        }

        b.iter(|| {
            for i in RANDOM_INDEXES.iter() {
                let _ = hash_index.rmw(&i, |val| {
                    val.x2 += 10;
                });
            }
            let _ = hash_index.persist().unwrap();
        });
    });
}

#[cfg(feature = "rocks")]
fn rmw_small_pure_rocks(b: &mut Bencher) {
    rmw_small_with_backends(BackendType::Rocks, b);
}

#[cfg(feature = "sled")]
fn rmw_small_pure_sled(b: &mut Bencher) {
    rmw_small_with_backends(BackendType::Sled, b);
}

fn rmw_small_with_backends(backend: BackendType, b: &mut Bencher) {
    let dir = tempdir().unwrap();
    with_backend_type!(backend, |B| {
        let backend = B::create(dir.as_ref()).unwrap();
        let mut map_handle: Handle<MapState<u64, SmallStruct>> = Handle::map("mapindex");
        let mut session = backend.session();
        {
            let mut rtok = unsafe { RegistrationToken::new(&mut session) };
            map_handle.register(&mut rtok);
        }

        let mut state = map_handle.activate(&mut session);
        // Fill in some keys
        for i in 0..TOTAL_KEYS {
            let _ = state.fast_insert(i, SmallStruct::new());
        }

        b.iter(|| {
            for i in RANDOM_INDEXES.iter() {
                let mut s = state.get(&i).unwrap().unwrap();
                s.x2 += 10;
                state.fast_insert(*i, s).unwrap()
            }
        });
    });
}

#[cfg(feature = "rocks")]
fn read_small_pure_rocks(b: &mut Bencher) {
    read_small_with_backends(BackendType::Rocks, b);
}
#[cfg(feature = "sled")]
fn read_small_pure_sled(b: &mut Bencher) {
    read_small_with_backends(BackendType::Sled, b);
}

fn read_small_with_backends(backend: BackendType, b: &mut Bencher) {
    let dir = tempdir().unwrap();
    with_backend_type!(backend, |B| {
        let backend = B::create(dir.as_ref()).unwrap();
        let mut map_handle: Handle<MapState<u64, SmallStruct>> = Handle::map("mapindex");
        let mut session = backend.session();
        {
            let mut rtok = unsafe { RegistrationToken::new(&mut session) };
            map_handle.register(&mut rtok);
        }

        let mut state = map_handle.activate(&mut session);
        // Fill in some keys
        for i in 0..TOTAL_KEYS {
            let _ = state.fast_insert(i, SmallStruct::new());
        }

        b.iter(|| {
            for i in RANDOM_INDEXES.iter() {
                assert_eq!(state.get(&i).unwrap().is_some(), true);
            }
        });
    });
}

criterion_group!(benches, hash);
criterion_main!(benches);
