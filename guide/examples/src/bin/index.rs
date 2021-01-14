use arcon::prelude::*;
use std::sync::Arc;

// ANCHOR: state
#[derive(ArconState)]
pub struct StreamState<B: Backend> {
    counter: Value<u64, B>,
    counters_map: HashTable<u64, String, B>,
    counters: Appender<u64, B>,
}

impl<B: Backend> StreamState<B> {
    pub fn new(backend: Arc<B>) -> Self {
        Self {
            counter: Value::new("_counter", backend.clone()),
            counters_map: HashTable::new("_counters_map", backend.clone()),
            counters: Appender::new("_counters", backend),
        }
    }
}
// ANCHOR_END: state

fn main() {
    let test_dir = tempfile::tempdir().unwrap();
    let path = test_dir.path();
    let backend = Arc::new(Sled::create(path).unwrap());

    let mut state = StreamState::new(backend);

    // ANCHOR: value
    // PUT, GET, RMW, and CLEAR

    state.counter().put(10);

    assert_eq!(state.counter().rmw(|v| *v += 10), true);
    assert_eq!(state.counter().get(), Some(&20));

    state.counter().clear();
    assert_eq!(state.counter().get(), None);

    // ANCHOR_END: value

    // ANCHOR: hash_table
    // PUT, GET, RMW, and REMOVE

    state
        .counters_map()
        .put(1, String::from("hello"))
        .expect("fail");

    assert!(state.counters_map().get(&5).unwrap().is_none());

    state
        .counters_map()
        .rmw(&1, || String::from("default"), |s| s.push_str(" world"))
        .expect("failure");

    assert_eq!(
        state.counters_map().get(&1).unwrap(),
        Some(&String::from("hello world"))
    );

    assert_eq!(
        state.counters_map().remove(&1).unwrap(),
        Some(String::from("hello world"))
    );

    // ANCHOR_END: hash_table

    // ANCHOR: appender
    // APPEND, CONSUME

    state.counters().append(1).expect("failure");
    state.counters().append(2).expect("failure");
    state.counters().append(3).expect("failure");

    assert_eq!(state.counters().consume().unwrap(), vec![1, 2, 3]);

    // ANCHOR_END: appender
}
