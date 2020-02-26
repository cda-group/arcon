// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use arcon::allocator::*;
use criterion::{black_box, criterion_group, Bencher, Criterion};

const ALLOC_SIZE: usize = 1024;

fn arcon_allocator(c: &mut Criterion) {
    let mut group = c.benchmark_group("arcon_allocator");
    group.bench_function("arcon_alloc", arcon_alloc);
    group.bench_function("rust_vec_alloc", rust_vec_alloc);

    group.finish()
}

fn arcon_alloc(b: &mut Bencher) {
    let mut a = ArconAllocator::new(81920);
    b.iter(|| {
        black_box({
            let (id, _) = unsafe { a.alloc::<u64>(ALLOC_SIZE).unwrap() };
            unsafe { a.dealloc(id) };
        });
    });
}

fn rust_vec_alloc(b: &mut Bencher) {
    b.iter(|| {
        black_box(Vec::<u64>::with_capacity(ALLOC_SIZE));
    });
}

criterion_group!(benches, arcon_allocator);
