use arcon::prelude::*;

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

#[arcon::app(debug = true)]
fn main() {
    (0..1000000)
        .map(|x| Event { id: x, data: 1.5 })
        .to_stream(|conf| {
            conf.set_timestamp_extractor(|x: &Event| x.id);
        })
        .key_by(|event: &Event| &event.id)
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
        .ignore()
}
