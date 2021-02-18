use arcon::prelude::*;
use examples::SnapshotComponent;
use std::sync::Arc;

#[cfg_attr(feature = "arcon_serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "unsafe_flight", derive(abomonation_derive::Abomonation))]
// ANCHOR: data
#[derive(Arcon, prost::Message, Copy, Clone)]
#[arcon(unsafe_ser_id = 12, reliable_ser_id = 13, version = 1, keys = "id")]
pub struct Event {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(float, tag = "2")]
    pub data: f32,
}
// ANCHOR_END: data

// ANCHOR: state
#[derive(ArconState)]
pub struct MyState<B: Backend> {
    events: EagerAppender<Event, B>,
}

impl<B: Backend> StateConstructor for MyState<B> {
    type BackendType = B;

    fn new(backend: Arc<Self::BackendType>) -> Self {
        Self {
            events: EagerAppender::new("_events", backend),
        }
    }
}

// ANCHOR_END: state

fn main() {
    let conf = ArconConf {
        epoch_interval: 10000,
        ..Default::default()
    };

    let mut pipeline = Pipeline::with_conf(conf)
        .collection(
            (0..10000000)
                .map(|x| Event { id: x, data: 1.5 })
                .collect::<Vec<Event>>(),
            |conf| {
                conf.set_timestamp_extractor(|x: &Event| x.id);
            },
        )
        .operator(OperatorBuilder {
            constructor: Arc::new(|backend| {
                Map::stateful(MyState::new(backend), |event, state| {
                    state.events().append(event)?;
                    Ok(event)
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
