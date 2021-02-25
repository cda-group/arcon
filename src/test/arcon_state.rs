// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::prelude::*;
use arcon_error::*;
use arcon_macros::ArconState;
use arcon_state::Backend;
use std::sync::Arc;

#[derive(ArconState)]
pub struct StreamingState<B: Backend> {
    watermark: LazyValue<u64, B>,
    epoch: LazyValue<u64, B>,
    counters: HashTable<u64, u64, B>,
    #[ephemeral]
    emph: u64,
}

impl<B: Backend> StateConstructor for StreamingState<B> {
    type BackendType = B;

    fn new(backend: Arc<Self::BackendType>) -> Self {
        Self {
            watermark: LazyValue::new("_watermark", backend.clone()),
            epoch: LazyValue::new("_epoch", backend.clone()),
            counters: HashTable::new("_counters", backend),
            emph: 0,
        }
    }
}

#[test]
fn streaming_state_test() -> ArconResult<()> {
    let backend = Arc::new(crate::test_utils::temp_backend());

    let mut state = StreamingState {
        watermark: LazyValue::new("_watermark", backend.clone()),
        epoch: LazyValue::new("_epoch", backend.clone()),
        counters: HashTable::new("_counters", backend),
        emph: 0,
    };

    state.watermark().put(100)?;
    state.epoch().put(1)?;
    state.counters().put(10, 1)?;
    state.counters().put(12, 2)?;

    let watermark = state.watermark().get()?;
    assert_eq!(watermark.unwrap().as_ref(), &100);
    let epoch = state.epoch().get()?;
    assert_eq!(epoch.unwrap().as_ref(), &1);
    assert_eq!(state.counters().get(&10).unwrap(), Some(&1));
    assert_eq!(state.counters().get(&12).unwrap(), Some(&2));
    assert_eq!(state.emph(), &0);
    assert_eq!(state.persist().is_ok(), true);
    Ok(())
}
