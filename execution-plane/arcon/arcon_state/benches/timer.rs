// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use arcon_state::{
    index::{timer::TimerIndex, IndexOps},
    Backend, *,
};
use criterion::{criterion_group, criterion_main, Bencher, Criterion, Throughput};
use once_cell::sync::Lazy;
use rand::Rng;
use tempfile::tempdir;

const TOTAL_KEYS: u64 = 10000;
const CAPACITY: [usize; 2] = [5048, 32768]; // Capacity in amount of elements and not as in bytes size..
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
    #[cfg(feature = "sled")]
    group.bench_function("Inserts Sled Backed", index_sled_backed);

    group.finish()
}

#[cfg(feature = "sled")]
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
        let backend = B::create(dir.as_ref()).unwrap();
        let mut index: TimerIndex<u64, u64, B> =
            TimerIndex::new(4096, std::sync::Arc::new(backend));
        b.iter(|| {
            for id in RANDOM_KEYS.iter() {
                assert_eq!(index.schedule_at(*id, 10, 1000).is_ok(), true);
            }
            //index.persist().unwrap()
        });
    });
}

// Writes
// Reads
// Write/Reads
// Recovery

criterion_group!(benches, timer);
criterion_main!(benches);
