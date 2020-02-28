// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use criterion::criterion_main;

mod benchmarks;

criterion_main! {
    benchmarks::arcon_node::benches,
    benchmarks::arcon_window::benches,
    benchmarks::arcon_serde::benches,
    benchmarks::arcon_flight_serde::benches,
    benchmarks::arcon_allocator::benches,
    benchmarks::arcon_buffer_pool::benches,
}
