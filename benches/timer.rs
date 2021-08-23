// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use arcon::prelude::{
    timer::{ArconTimer, Timer},
    Backend,
};
use arcon_state::{with_backend_type, BackendType};
use criterion::{criterion_group, criterion_main, Bencher, Criterion, Throughput};
use once_cell::sync::Lazy;
use rand::Rng;
use std::sync::Arc;
use tempfile::tempdir;

const TOTAL_KEYS: u64 = 10000;
const TOTAL_OPERATIONS: u64 = 10000;

static RANDOM_KEYS: Lazy<Vec<u64>> = Lazy::new(|| {
    let mut rng = rand::thread_rng();
    let mut indexes = Vec::with_capacity(TOTAL_OPERATIONS as usize);
    for _i in 0..TOTAL_OPERATIONS {
        indexes.push(rng.gen_range(0, TOTAL_KEYS));
    }
    indexes
});

fn timer(c: &mut Criterion) {
    let mut group = c.benchmark_group("timer");
    group.throughput(Throughput::Elements(TOTAL_OPERATIONS));

    #[cfg(feature = "rocks")]
    group.bench_function("Inserts Rocks Backed", index_rocks_backed);
    group.bench_function("Inserts Sled Backed", index_sled_backed);

    group.finish()
}

fn index_sled_backed(b: &mut Bencher) {
    timer_inserts(BackendType::Sled, b);
}

#[cfg(feature = "rocks")]
fn index_rocks_backed(b: &mut Bencher) {
    timer_inserts(BackendType::Rocks, b);
}

fn timer_inserts(backend: BackendType, b: &mut Bencher) {
    let dir = tempdir().unwrap();
    with_backend_type!(backend, |B| {
        let backend = Arc::new(B::create(dir.as_ref(), "timer_backend".to_string()).unwrap());
        let mut index: Timer<u64, u64, B> = Timer::new("_timer", backend);
        b.iter(|| {
            for id in RANDOM_KEYS.iter() {
                assert!(index.schedule_at(*id, 10, 1000).is_ok());
            }
        });
    });
}

// Writes
// Reads
// Write/Reads
// Recovery

criterion_group!(benches, timer);
criterion_main!(benches);
