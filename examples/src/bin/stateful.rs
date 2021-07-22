use arcon::prelude::*;
use std::sync::Arc;

#[cfg_attr(feature = "arcon_serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "unsafe_flight", derive(abomonation_derive::Abomonation))]
#[derive(Arcon, Arrow, prost::Message, Copy, Clone)]
#[arcon(unsafe_ser_id = 12, reliable_ser_id = 13, version = 1, keys = "id")]
pub struct Event {
    #[prost(uint64)]
    pub id: u64,
    #[prost(float)]
    pub data: f32,
}

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

fn main() {
    let conf = ArconConf {
        epoch_interval: 2500,
        ctrl_system_host: Some("127.0.0.1:2000".to_string()),
        ..Default::default()
    };

    let mut pipeline = Pipeline::with_conf(conf)
        .collection(
            (0..1000000)
                .map(|x| Event { id: x, data: 1.5 })
                .collect::<Vec<Event>>(),
            |conf| {
                conf.set_timestamp_extractor(|x: &Event| x.id);
            },
        )
        .operator(OperatorBuilder {
            constructor: Arc::new(|backend: Arc<Sled>| {
                Map::stateful(MyState::new(backend), |event, state| {
                    state.events().put(event)?;
                    Ok(event)
                })
            }),
            conf: Default::default(),
        })
        .build();

    pipeline.start();
    pipeline.await_termination();
}
