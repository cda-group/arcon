use arcon_allocator::{Alloc, Allocator};
use criterion::{black_box, criterion_group, criterion_main, Bencher, Criterion};

const ALLOC_SIZE: usize = 1024;

fn arcon_allocator(c: &mut Criterion) {
    let mut group = c.benchmark_group("arcon_allocator");
    group.bench_function("arcon_alloc", arcon_alloc);
    group.bench_function("rust_vec_alloc", rust_vec_alloc);

    group.finish()
}

fn arcon_alloc(b: &mut Bencher) {
    let mut a = Allocator::new(81920);
    b.iter(|| {
        if let Ok(Alloc(id, _)) = unsafe { a.alloc::<u64>(ALLOC_SIZE) } {
            unsafe { a.dealloc(id) };
        }
    });
}

fn rust_vec_alloc(b: &mut Bencher) {
    b.iter(|| {
        black_box(Vec::<u64>::with_capacity(ALLOC_SIZE));
    });
}

criterion_group!(benches, arcon_allocator);
criterion_main!(benches);
