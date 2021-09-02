use crate::prelude::*;
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

#[test]
fn streaming_state_test() -> ArconResult<()> {
    let backend = Arc::new(crate::test_utils::temp_backend::<Sled>());

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
    assert!(state.persist().is_ok());
    Ok(())
}
