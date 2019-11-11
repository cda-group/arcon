use criterion::criterion_main;

mod benchmarks;

criterion_main! {
    benchmarks::arcon_node::benches,
    benchmarks::arcon_window::benches,
    benchmarks::arcon_serde::benches,
}
