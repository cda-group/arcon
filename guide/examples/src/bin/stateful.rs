use arcon::prelude::*;
use std::{path::Path, sync::Arc};

#[cfg_attr(feature = "arcon_serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "unsafe_flight", derive(abomonation_derive::Abomonation))]
// ANCHOR: data
#[derive(Arcon, prost::Message, Copy, Clone)]
#[arcon(unsafe_ser_id = 12, reliable_ser_id = 13, version = 1)]
pub struct Event {
    #[prost(uint64, tag = "1")]
    pub id: u64,
}
// ANCHOR_END: data

// ANCHOR: state
#[derive(ArconState)]
pub struct MyState<B: Backend> {
    events: Appender<Event, B>,
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

// ANCHOR_END: state

impl<B: Backend> From<Snapshot> for MyState<B> {
    fn from(snapshot: Snapshot) -> Self {
        let snapshot_dir = Path::new(&snapshot.snapshot_path);
        let backend = Arc::new(B::restore(&snapshot_dir, &snapshot_dir).unwrap());
        MyState::new(backend)
    }
}

fn main() {
    let conf = ArconConf {
        epoch_interval: 10000,
        ..Default::default()
    };

    let mut pipeline = Pipeline::with_conf(conf)
        .collection(
            (0..10000000)
                .map(|x| Event { id: x })
                .collect::<Vec<Event>>(),
            |conf| {
                conf.set_timestamp_extractor(|x: &Event| x.id);
            },
        )
        .filter(|event| event.id > 50)
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
