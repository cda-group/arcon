// Modifications Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use arcon_state::{
    backend::{Backend, Handle, Sled},
    index::{IndexOps, Map, Value},
    ArconState,
};

#[derive(ArconState)]
pub struct StreamingState<B: Backend> {
    watermark: Value<u64, B>,
    epoch: Value<u64, B>,
    counters: Map<u64, u64, B>,
    #[ephemeral]
    emph: u64,
}

#[test]
fn streaming_state_test() {
    let b = Sled::create(&std::path::Path::new("/tmp/state_test")).unwrap();
    let backend = std::sync::Arc::new(b);
    let mut watermark_handle = Handle::value("_watermark");
    let mut epoch_handle = Handle::value("_epoch");
    let mut counters_handle = Handle::map("_counters");

    backend.register_value_handle(&mut watermark_handle);
    backend.register_value_handle(&mut epoch_handle);
    backend.register_map_handle(&mut counters_handle);

    let active_watermark_handle = watermark_handle.activate(backend.clone());
    let active_epoch_handle = epoch_handle.activate(backend.clone());
    let active_counters_handle = counters_handle.activate(backend.clone());

    let mut state = StreamingState {
        watermark: Value::new(active_watermark_handle),
        epoch: Value::new(active_epoch_handle),
        counters: Map::new(active_counters_handle),
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
