use arcon::prelude::*;
use std::sync::Arc;

#[derive(ArconState)]
pub struct MyState<B: Backend> {
    events: Appender<u64, B>,
}

impl<B: Backend> MyState<B> {
    pub fn new(backend: Arc<B>) -> Self {
        let mut events_handle = Handle::vec("_events");
        backend.register_vec_handle(&mut events_handle);

        MyState {
            events: Appender::new(events_handle.activate(backend)),
        }
    }
}

fn main() {
    let pipeline = Pipeline::default()
        .collection((0..100).collect::<Vec<u64>>())
        .filter(|x: &u64| *x > 50)
        .map_with_state(
            |x: u64, state: &mut MyState<Sled>| {
                state.events().append(x)?;
                Ok(x)
            },
            MyState::new,
            |conf| {
                conf.set_state_id("map_state_one");
            },
        )
        .to_console()
        .build();

    pipeline.start();
    pipeline.await_termination();
}
