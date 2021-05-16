use arcon::prelude::*;

fn arrow_udf(schema: Arc<Schema>, batches: Vec<RecordBatch>) -> OperatorResult<u64> {
    Ok(1)
}

#[cfg_attr(feature = "arcon_serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "unsafe_flight", derive(abomonation_derive::Abomonation))]
// ANCHOR: data
#[derive(Arcon, Arrow, prost::Message, Copy, Clone)]
#[arcon(unsafe_ser_id = 12, reliable_ser_id = 13, version = 1)]
pub struct Event {
    #[prost(uint64)]
    pub data: u64,
}

fn main() {
    let mut pipeline = Pipeline::default()
        .collection((0..100000).map(|i| Event { data: i}).collect::<Vec<Event>>(), |conf| {
            conf.set_arcon_time(ArconTime::Event);
            conf.set_timestamp_extractor(|x: &Event| x.data);
        })
        .operator(OperatorBuilder {
            constructor: Arc::new(|backend| {
                let function = ArrowWindow::new(backend.clone(), &arrow_udf);
                WindowAssigner::tumbling(
                    function,
                    backend,
                    Time::seconds(2000),
                    Time::seconds(0),
                    false,
                )
            }),
            conf: Default::default(),
        })
        .to_console()
        .build();

    pipeline.start();
    pipeline.await_termination();
}
