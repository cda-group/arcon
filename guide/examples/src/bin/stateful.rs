use arcon::prelude::*;
use examples::SnapshotComponent;
use std::sync::Arc;

#[cfg_attr(feature = "arcon_serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "unsafe_flight", derive(abomonation_derive::Abomonation))]
// ANCHOR: data
#[derive(Arcon, Arrow, prost::Message, Copy, Clone)]
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
    #[table = "events"]
    events: EagerValue<Event, B>,
}

impl<B: Backend> StateConstructor for MyState<B> {
    type BackendType = B;

    fn new(backend: Arc<Self::BackendType>) -> Self {
        Self {
            events: EagerValue::new("_events", backend),
        }
    }
}

// ANCHOR_END: state

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let conf = ArconConf {
        epoch_interval: 10000,
        endpoint_host: Some("127.0.0.1:2000".to_string()),
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
                    state.events().put(event)?;
                    Ok(event)
                })
            }),
            conf: Default::default(),
        })
        .build();

    // ANCHOR: watch_thread

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

    Ok(())
}
