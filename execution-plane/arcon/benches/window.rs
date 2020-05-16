// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

// Benchmarks for different Window types

use arcon::{
    prelude::*,
    state::{InMemory, Rocks},
    stream::operator::window::WindowContext,
};
use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};

const WINDOW_MSGS: usize = 1000;

fn arcon_window_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("arcon_window_latency");
    group.bench_function("AppenderWindow sum", window_appender_sum);
    #[cfg(feature = "arcon_rocksdb")]
    group.bench_function("AppenderWindow sum RocksDB", window_appender_sum_rocksdb);
    group.bench_function("IncrementalWindow sum", window_incremental_sum);
    #[cfg(feature = "arcon_rocksdb")]
    group.bench_function(
        "IncrementalWindow sum RocksDB",
        window_incremental_sum_rocksdb,
    );
    group.bench_function("AppenderWindow Sum Square", window_appender_sum_square);
    #[cfg(feature = "rayon")]
    group.bench_function(
        "AppenderWindow Sum Square Parallel",
        window_appender_sum_square_par,
    );

    group.finish()
}

pub fn window_appender_sum(b: &mut Bencher) {
    let mut state_backend = InMemory::create("bench".as_ref()).unwrap();
    let mut state_session = state_backend.session();
    b.iter(|| appender_sum(black_box(WINDOW_MSGS), &mut state_session));
}

#[cfg(feature = "arcon_rocksdb")]
pub fn window_appender_sum_rocksdb(b: &mut Bencher) {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let test_directory = temp_dir.path();
    let mut state_backend = Rocks::create(test_directory).unwrap();
    let mut state_session = state_backend.session();
    b.iter(|| appender_sum(black_box(WINDOW_MSGS), &mut state_session));
}

pub fn appender_sum<SB: state::Backend>(messages: usize, state_session: &mut state::Session<SB>) {
    #[inline]
    fn materializer(buffer: &[u64]) -> u64 {
        buffer.iter().sum()
    }
    let mut window: AppenderWindow<u64, u64> = AppenderWindow::new(&materializer);
    window.register_states(&mut unsafe { state::RegistrationToken::new(state_session) });
    for i in 0..messages {
        let _ = window.on_element(i as u64, WindowContext::new(state_session, 0, 0));
    }
    let s: u64 = window
        .result(WindowContext::new(state_session, 0, 0))
        .unwrap();
    assert!(s > 0);
}

pub fn window_incremental_sum(b: &mut Bencher) {
    let mut state_backend = InMemory::create("bench".as_ref()).unwrap();
    let mut state_session = state_backend.session();
    b.iter(|| incremental_sum(black_box(WINDOW_MSGS), &mut state_session));
}

#[cfg(feature = "arcon_rocksdb")]
pub fn window_incremental_sum_rocksdb(b: &mut Bencher) {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let test_directory = temp_dir.path();
    let mut state_backend = Rocks::create(test_directory).unwrap();
    let mut state_session = state_backend.session();
    b.iter(|| incremental_sum(black_box(WINDOW_MSGS), &mut state_session));
}

pub fn incremental_sum<SB: state::Backend>(
    messages: usize,
    state_session: &mut state::Session<SB>,
) {
    #[inline]
    fn init(i: u64) -> u64 {
        i
    }

    #[inline]
    fn aggregation(i: u64, agg: &u64) -> u64 {
        agg + i
    }

    let mut window: IncrementalWindow<u64, u64> = IncrementalWindow::new(&init, &aggregation);
    window.register_states(&mut unsafe { state::RegistrationToken::new(state_session) });

    for i in 0..messages {
        let _ = window.on_element(i as u64, WindowContext::new(state_session, 0, 0));
    }

    let s: u64 = window
        .result(WindowContext::new(state_session, 0, 0))
        .unwrap();
    assert!(s > 0);
}

pub fn window_appender_sum_square(b: &mut Bencher) {
    b.iter(|| sum_square(black_box(WINDOW_MSGS)));
}

#[cfg(feature = "rayon")]
pub fn window_appender_sum_square_par(b: &mut Bencher) {
    b.iter(|| sum_square_par(black_box(WINDOW_MSGS)));
}

pub fn sum_square(messages: usize) {
    let mut state_backend = InMemory::create("bench".as_ref()).unwrap();
    let mut state_session = state_backend.session();

    #[inline]
    fn materializer(buffer: &[u64]) -> u64 {
        buffer.iter().map(|&x| x * x).sum()
    }
    let mut window: AppenderWindow<u64, u64> = AppenderWindow::new(&materializer);
    window.register_states(&mut unsafe { state::RegistrationToken::new(&mut state_session) });

    for i in 0..messages {
        let _ = window.on_element(i as u64, WindowContext::new(&mut state_session, 0, 0));
    }
    let s: u64 = window
        .result(WindowContext::new(&mut state_session, 0, 0))
        .unwrap();
    assert!(s > 0);
}

#[cfg(feature = "rayon")]
pub fn sum_square_par(messages: usize) {
    let mut state_backend = InMemory::create("bench".as_ref()).unwrap();
    let mut state_session = state_backend.session();

    #[inline]
    fn materializer(buffer: &[u64]) -> u64 {
        buffer.par_iter().map(|&x| x * x).sum()
    }
    let mut window: AppenderWindow<u64, u64> = AppenderWindow::new(&materializer);
    window.register_states(&mut unsafe { state::RegistrationToken::new(&mut state_session) });

    for i in 0..messages {
        let _ = window.on_element(i as u64, WindowContext::new(&mut state_session, 0, 0));
    }
    let s: u64 = window
        .result(WindowContext::new(&mut state_session, 0, 0))
        .unwrap();
    assert!(s > 0);
}

criterion_group!(benches, arcon_window_latency);
criterion_main!(benches);
