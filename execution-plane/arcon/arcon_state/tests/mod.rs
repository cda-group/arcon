// Modifications Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use arcon_state::{
    backend::{Backend, InMemory},
    index::{HashIndex, IndexOps, ValueIndex},
    ArconState,
};

#[derive(ArconState)]
pub struct StreamingState<B: Backend> {
    watermark: ValueIndex<u64, B>,
    epoch: ValueIndex<u64, B>,
    counters: HashIndex<u64, u64, B>,
}

#[test]
fn streaming_state_test() {
    let b = InMemory::create(&std::path::Path::new("/tmp/")).unwrap();
    let backend = std::sync::Arc::new(b);
    let mut state = StreamingState {
        watermark: ValueIndex::new("_watermark", backend.clone()),
        epoch: ValueIndex::new("_epoch", backend.clone()),
        counters: HashIndex::new("_counters", 1024, 1024, backend.clone()),
    };

    state.watermark().put(100);
    state.epoch().put(1);
    state.counters().put(10, 1).unwrap();
    state.counters().put(12, 2).unwrap();

    assert_eq!(state.watermark().get(), Some(&100));
    assert_eq!(state.epoch().get(), Some(&1));
    assert_eq!(state.counters().get(&10).unwrap(), Some(&1));
    assert_eq!(state.counters().get(&12).unwrap(), Some(&2));
    assert_eq!(state.persist().is_ok(), true);
}
