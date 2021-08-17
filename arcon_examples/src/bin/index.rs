use arcon::prelude::*;
use std::sync::Arc;

#[derive(ArconState)]
pub struct StreamState<B: Backend> {
    counter: LazyValue<u64, B>,
    counters_map: HashTable<u64, String, B>,
    counters: EagerAppender<u64, B>,
}

impl<B: Backend> StateConstructor for StreamState<B> {
    type BackendType = B;

    fn new(backend: Arc<Self::BackendType>) -> Self {
        Self {
            counter: LazyValue::new("_counter", backend.clone()),
            counters_map: HashTable::new("_counters_map", backend.clone()),
            counters: EagerAppender::new("_counters", backend),
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let test_dir = tempfile::tempdir().unwrap();
    let path = test_dir.path();
    let backend = Arc::new(Sled::create(path, String::from("testDB")).unwrap());

    let mut state = StreamState::new(backend);

    // PUT, GET, RMW, and CLEAR

    state.counter().put(10)?;

    assert!(state.counter().rmw(|v| *v += 10).is_ok());

    let counter = state.counter().get()?;
    assert_eq!(counter.unwrap().as_ref(), &20);

    state.counter().clear()?;

    let counter = state.counter().get()?;
    assert_eq!(counter, None);

    // PUT, GET, RMW, and REMOVE

    state.counters_map().put(1, String::from("hello"))?;

    assert!(state.counters_map().get(&5).unwrap().is_none());

    state
        .counters_map()
        .rmw(&1, || String::from("default"), |s| s.push_str(" world"))?;

    assert_eq!(
        state.counters_map().get(&1).unwrap(),
        Some(&String::from("hello world"))
    );

    assert_eq!(
        state.counters_map().remove(&1).unwrap(),
        Some(String::from("hello world"))
    );

    // APPEND, CONSUME

    state.counters().append(1)?;
    state.counters().append(2)?;
    state.counters().append(3)?;

    assert_eq!(state.counters().consume().unwrap(), vec![1, 2, 3]);

    Ok(())
}
