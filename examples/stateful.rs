use arcon::prelude::*;
use std::{path::Path, sync::Arc};

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
        let snapshot_dir = Path::new(&snapshot.snapshot.snapshot_path);
        let backend = Arc::new(B::restore(&snapshot_dir, &snapshot_dir).unwrap());
        MyState::new(backend)
    }
}

fn main() {
    let mut conf = ArconConf::default();
    conf.epoch_interval = 10000;

    let mut pipeline = Pipeline::with_conf(conf)
        .collection((0..10000000).collect::<Vec<u64>>())
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
        .build();

    pipeline.watch("map_state_one", |epoch, mut s: MyState<Sled>| {
        let events_len = s.events().len();
        println!("Events Length {:?} for Epoch {}", events_len, epoch);
    });

    pipeline.start();
    pipeline.await_termination();
}
