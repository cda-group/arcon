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

impl<B: Backend> From<SnapshotRef> for MyState<B> {
    fn from(snapshot: SnapshotRef) -> Self {
        let s1_snapshot_dir = std::path::Path::new(&snapshot.snapshot.snapshot_path);
        let backend = Arc::new(B::restore(&s1_snapshot_dir, &s1_snapshot_dir).unwrap());
        MyState::new(backend)
    }
}

fn main() {
    let mut pipeline = Pipeline::default()
        .collection((0..100).collect::<Vec<u64>>())
        .filter(|x| *x > 50)
        .map_with_state(
            |x, state: &mut MyState<Sled>| {
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

    pipeline.watch("map_state_one", |epoch, _: MyState<Sled>| {
        println!("Got State For Epoch {}", epoch);
    });

    pipeline.start();
    pipeline.await_termination();
}
