// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use arcon::bench_utils::{BufferPool, BufferReader};
use arcon_allocator::Allocator;
use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};
use std::sync::{
    mpsc::{channel, Receiver, Sender},
    Arc, Mutex,
};

const ALLOCATOR_BYTES: usize = 1073741824; // 1GB
const SMALL_BUFFER: usize = 248;
const LARGE_BUFFER: usize = 4048;
const THREAD_ITERATIONS: usize = 16;
const POOL_CAPACITY: usize = 8;

fn arcon_buffer_pool(c: &mut Criterion) {
    let mut group = c.benchmark_group("arcon_buffer_pool");
    group.bench_function(
        "BufferPool single thread small buffer",
        pool_single_thread_small,
    );
    group.bench_function(
        "Rust Vec single thread small buffer",
        rust_single_thread_small,
    );
    group.bench_function(
        "BufferPool single thread large buffer",
        pool_single_thread_large,
    );
    group.bench_function(
        "Rust Vec single thread large buffer",
        rust_single_thread_large,
    );

    group.bench_function(
        "Rust Vec multi-threaded small buffer",
        rust_multi_threaded_small,
    );
    group.bench_function(
        "BufferPool multi-threaded small buffer",
        pool_multi_threaded_small,
    );
    group.bench_function(
        "Rust Vec multi-threaded large buffer",
        rust_multi_threaded_large,
    );
    group.bench_function(
        "BufferPool multi-threaded large buffer",
        pool_multi_threaded_large,
    );
    group.finish()
}

fn pool_single_thread_small(b: &mut Bencher) {
    buffer_pool_single_thread(b, SMALL_BUFFER);
}

fn pool_single_thread_large(b: &mut Bencher) {
    buffer_pool_single_thread(b, LARGE_BUFFER);
}

fn pool_multi_threaded_small(b: &mut Bencher) {
    b.iter(|| buffer_pool_multi_threaded(SMALL_BUFFER));
}

fn pool_multi_threaded_large(b: &mut Bencher) {
    b.iter(|| buffer_pool_multi_threaded(LARGE_BUFFER));
}

fn buffer_pool_single_thread(b: &mut Bencher, buffer_size: usize) {
    let allocator = Arc::new(Mutex::new(Allocator::new(ALLOCATOR_BYTES)));
    let mut pool: BufferPool<u64> = BufferPool::new(POOL_CAPACITY, buffer_size, allocator).unwrap();

    b.iter(|| {
        // fetch BufferWriter
        let mut writer = pool.get();
        for i in 0..buffer_size {
            black_box(writer.push(i as u64));
        }
        // Create BufferReader
        let reader = writer.reader();
        for item in reader.into_iter() {
            let _ = black_box(item);
        }
        // EventBuffer is returned to the pool
    });
}

fn buffer_pool_multi_threaded(buffer_size: usize) {
    let allocator = Arc::new(Mutex::new(Allocator::new(ALLOCATOR_BYTES)));
    let mut pool: BufferPool<u64> = BufferPool::new(POOL_CAPACITY, buffer_size, allocator).unwrap();

    let (tx, rx): (Sender<BufferReader<u64>>, Receiver<BufferReader<u64>>) = channel();

    let receiver = std::thread::spawn(move || {
        for _ in 0..THREAD_ITERATIONS {
            let reader = rx.recv().unwrap();
            black_box(reader);
            // For multi-threaded case, skip reading
        }
    });

    for _ in 0..THREAD_ITERATIONS {
        // fetch BufferWriter
        let mut writer = pool.get();
        for i in 0..buffer_size {
            black_box(writer.push(i as u64));
        }
        let reader = writer.reader();
        tx.send(reader).unwrap();
    }

    receiver.join().unwrap();
}

fn rust_single_thread_small(b: &mut Bencher) {
    rust_vec_single_thread(b, SMALL_BUFFER);
}

fn rust_single_thread_large(b: &mut Bencher) {
    rust_vec_single_thread(b, LARGE_BUFFER);
}

fn rust_vec_single_thread(b: &mut Bencher, buffer_size: usize) {
    b.iter(|| {
        let mut vec: Vec<u64> = Vec::with_capacity(buffer_size);
        for i in 0..buffer_size {
            vec.push(i as u64);
        }
        for i in 0..buffer_size {
            let _ = black_box(vec.get(i).unwrap());
        }
    });
}

fn rust_multi_threaded_small(b: &mut Bencher) {
    b.iter(|| rust_vec_multi_threaded(SMALL_BUFFER));
}

fn rust_multi_threaded_large(b: &mut Bencher) {
    b.iter(|| rust_vec_multi_threaded(LARGE_BUFFER));
}

fn rust_vec_multi_threaded(buffer_size: usize) {
    let (tx, rx): (Sender<Vec<u64>>, Receiver<Vec<u64>>) = channel();
    let receiver = std::thread::spawn(move || {
        for _ in 0..THREAD_ITERATIONS {
            let vec = rx.recv().unwrap();
            black_box(vec);
            // For multi-threaded case, skip reading
        }
    });

    for _ in 0..THREAD_ITERATIONS {
        let mut vec: Vec<u64> = Vec::with_capacity(buffer_size);
        for i in 0..buffer_size {
            vec.push(i as u64);
        }
        tx.send(vec).unwrap();
    }

    receiver.join().unwrap();
}

criterion_group!(benches, arcon_buffer_pool);
criterion_main!(benches);
