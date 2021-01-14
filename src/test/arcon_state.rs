// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use arcon_state::{
    index::{IndexOps, HashTable, Value},
    ArconState,
};

#[derive(ArconState)]
pub struct StreamingState<B: Backend> {
    watermark: Value<u64, B>,
    epoch: Value<u64, B>,
    counters: HashTable<u64, u64, B>,
    #[ephemeral]
    emph: u64,
}

#[test]
fn streaming_state_test() {
    let backend = std::sync::Arc::new(crate::util::temp_backend());

    let mut state = StreamingState {
        watermark: Value::new("_watermark", backend.clone()),
        epoch: Value::new("_epoch", backend.clone()),
        counters: HashTable::new("_counters", backend),
        emph: 0,
    };

    state.watermark().put(100);
    state.epoch().put(1);
    state.counters().put(10, 1).unwrap();
    state.counters().put(12, 2).unwrap();

    assert_eq!(state.watermark().get(), Some(&100));
    assert_eq!(state.epoch().get(), Some(&1));
    assert_eq!(state.counters().get(&10).unwrap(), Some(&1));
    assert_eq!(state.counters().get(&12).unwrap(), Some(&2));
    assert_eq!(state.emph(), &0);
    assert_eq!(state.persist().is_ok(), true);
}
