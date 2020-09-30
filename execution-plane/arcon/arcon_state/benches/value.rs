// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use arcon_state::{
    index::{value::ValueIndex, IndexOps},
    Aggregator, Backend, *,
};
use criterion::{criterion_group, criterion_main, Bencher, Criterion, Throughput};
use tempfile::tempdir;

const OPS_PER_EPOCH: u64 = 10000;

fn value(c: &mut Criterion) {
    let mut group = c.benchmark_group("value");
    group.throughput(Throughput::Elements(OPS_PER_EPOCH));
    #[cfg(feature = "rocks")]
    group.bench_function("rolling counter index rocks backed", index_rocks_backed);
    #[cfg(feature = "sled")]
    group.bench_function("rolling counter index sled backed", index_sled_backed);
    #[cfg(feature = "rocks")]
    group.bench_function("rolling counter naive pure rocks", naive_rolling_rocks);
    #[cfg(feature = "sled")]
    group.bench_function("rolling counter naive pure sled", naive_rolling_sled);
    #[cfg(feature = "rocks")]
    group.bench_function("specialised rocks", specialised_rocks);
    #[cfg(feature = "sled")]
    group.bench_function("specialised sled", specialised_sled);

    group.finish()
}

#[cfg(feature = "sled")]
fn index_sled_backed(b: &mut Bencher) {
    index_rolling_counter(BackendType::Sled, b);
}

#[cfg(feature = "rocks")]
fn index_rocks_backed(b: &mut Bencher) {
    index_rolling_counter(BackendType::Rocks, b);
}

fn index_rolling_counter(backend: BackendType, b: &mut Bencher) {
    let dir = tempdir().unwrap();
    with_backend_type!(backend, |B| {
        let backend = B::create(dir.as_ref()).unwrap();
        let mut value_index: ValueIndex<u64, B> =
            ValueIndex::new("_valueindex", std::sync::Arc::new(backend));
        b.iter(|| {
            let curr_value = value_index.get().unwrap().clone();
            for _i in 0..OPS_PER_EPOCH {
                value_index.rmw(|v| {
                    *v += 1;
                });
            }
            let new_value = value_index.get().unwrap();
            assert_eq!(new_value, &(curr_value + OPS_PER_EPOCH));
            // simulate an epoch and persist the value index
            value_index.persist()
        });
    });
}

#[derive(Debug, Clone)]
pub struct CounterAggregator;
impl Aggregator for CounterAggregator {
    type Input = u64;
    type Accumulator = u64;
    type Result = u64;

    fn create_accumulator(&self) -> Self::Accumulator {
        0
    }

    fn add(&self, acc: &mut Self::Accumulator, value: Self::Input) {
        *acc += value;
    }

    fn merge_accumulators(
        &self,
        mut fst: Self::Accumulator,
        snd: Self::Accumulator,
    ) -> Self::Accumulator {
        fst += snd;
        fst
    }

    fn accumulator_into_result(&self, acc: Self::Accumulator) -> Self::Result {
        acc
    }
}

#[cfg(feature = "rocks")]
fn naive_rolling_rocks(b: &mut Bencher) {
    naive_rolling_counter(BackendType::Rocks, b);
}

#[cfg(feature = "sled")]
fn naive_rolling_sled(b: &mut Bencher) {
    naive_rolling_counter(BackendType::Sled, b);
}

fn naive_rolling_counter(backend: BackendType, b: &mut Bencher) {
    let dir = tempdir().unwrap();
    with_backend_type!(backend, |B| {
        let backend = B::create(dir.as_ref()).unwrap();
        let mut value_handle: Handle<ValueState<u64>> = Handle::value("_valueindex");
        let mut session = backend.session();
        {
            let mut rtok = unsafe { RegistrationToken::new(&mut session) };
            value_handle.register(&mut rtok);
        }

        let mut state = value_handle.activate(&mut session);
        b.iter(|| {
            let curr_value: u64 = state.get().unwrap().unwrap_or(0);
            for _i in 0..OPS_PER_EPOCH {
                let mut counter: u64 = state.get().unwrap().unwrap_or(0);
                counter += 1;
                state.fast_set(counter).unwrap();
            }
            let new_value: u64 = state.get().unwrap().unwrap_or(0);
            assert_eq!(new_value, curr_value + OPS_PER_EPOCH);
        });
    });
}

#[cfg(feature = "rocks")]
fn specialised_rocks(b: &mut Bencher) {
    specialised_rolling_counter(BackendType::Rocks, b);
}

#[cfg(feature = "sled")]
fn specialised_sled(b: &mut Bencher) {
    specialised_rolling_counter(BackendType::Sled, b);
}

fn specialised_rolling_counter(backend: BackendType, b: &mut Bencher) {
    let dir = tempdir().unwrap();
    with_backend_type!(backend, |B| {
        let backend = B::create(dir.as_ref()).unwrap();
        let mut agg_handle = Handle::aggregator("agger", CounterAggregator);
        let mut session = backend.session();

        {
            let mut rtok = unsafe { RegistrationToken::new(&mut session) };
            agg_handle.register(&mut rtok);
        }

        let mut state = agg_handle.activate(&mut session);
        b.iter(|| {
            let curr_value: u64 = state.get().unwrap();
            for _i in 0..OPS_PER_EPOCH {
                let _ = state.aggregate(1).unwrap();
            }
            let new_value: u64 = state.get().unwrap();
            assert_eq!(new_value, curr_value + OPS_PER_EPOCH);
        });
    });
}

criterion_group!(benches, value);
criterion_main!(benches);
