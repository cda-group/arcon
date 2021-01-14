use arcon::prelude::*;
use examples::SnapshotComponent;
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
        MyState {
            events: Appender::new("_events", backend),
        }
    }
}

// ANCHOR_END: state

// ANCHOR: snapshot
impl<B: Backend> From<Snapshot> for MyState<B> {
    fn from(snapshot: Snapshot) -> Self {
        let snapshot_dir = Path::new(&snapshot.snapshot_path);
        let backend = Arc::new(B::restore(&snapshot_dir, &snapshot_dir).unwrap());
        MyState::new(backend)
    }
}
// ANCHOR_END: snapshot

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
        .operator(OperatorBuilder {
            constructor: Arc::new(|backend| {
                Map::stateful(MyState::new(backend), |x, state| {
                    state.events().append(x)?;
                    Ok(x)
                })
            }),
            conf: Default::default(),
        })
        .build();

    // ANCHOR: watch_thread
    pipeline.watch(|epoch, mut s: MyState<Sled>| {
        let events_len = s.events().len();
        println!("Events Length {:?} for Epoch {}", events_len, epoch);
    });
    // ANCHOR_END: watch_thread

    // ANCHOR: watch_component
    let kompact_system = KompactConfig::default().build().expect("KompactSystem");
    // Create Kompact component to receive snapshots
    let snapshot_comp = kompact_system.create(SnapshotComponent::new);

    kompact_system
        .start_notify(&snapshot_comp)
        .wait_timeout(std::time::Duration::from_millis(200))
        .expect("Failed to start component");

    pipeline.watch_with::<MyState<Sled>>(snapshot_comp);
    // ANCHOR_END: watch_component

    pipeline.start();
    pipeline.await_termination();
}
