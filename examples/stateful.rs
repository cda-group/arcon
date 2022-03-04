use arcon::prelude::*;

#[arcon::proto]
#[derive(Arcon, Arrow, Copy, Clone)]
pub struct Event {
    pub id: u64,
    pub data: f32,
}

#[derive(ArconState)]
pub struct MyState<B: Backend> {
    #[table = "events"]
    events: EagerValue<Event, B>,
}

#[arcon::app]
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
