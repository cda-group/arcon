use arcon::prelude::*;
use std::sync::Arc;

#[derive(Arcon, Arrow, prost::Message, Copy, Clone)]
#[arcon(unsafe_ser_id = 12, reliable_ser_id = 13, version = 1)]
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

fn main() {
    let conf = ApplicationConf {
        epoch_interval: 2500,
        ctrl_system_host: Some("127.0.0.1:2000".to_string()),
        ..Default::default()
    };

    let mut app = Application::with_conf(conf)
        .iterator((0..1000000).map(|x| Event { id: x, data: 1.5 }), |conf| {
            conf.set_timestamp_extractor(|x: &Event| x.id);
        })
        //.key_by(Arc::new(|event: &Event| &event.id)) TODO
        .operator(OperatorBuilder {
            operator: Arc::new(|| {
                Map::stateful(|event, state: &mut MyState<_>| {
                    state.events().put(event)?;
                    Ok(event)
                })
            }),
            state: Arc::new(|backend| MyState {
                events: EagerValue::new("_events", backend),
            }),
            conf: Default::default(),
        })
        .build();

    app.start();
    app.await_termination();
}
