use arcon::prelude::*;
use tokio::runtime::Runtime;

fn arrow_udf(schema: Arc<Schema>, batches: Vec<RecordBatch>) -> ArconResult<u64> {
    let mut ctx = ExecutionContext::new();
    let provider = MemTable::try_new(schema, vec![batches]).unwrap();
    ctx.register_table("t", Arc::new(provider)).unwrap();
    let df = ctx.table("t").unwrap();
    let group_expr = vec![col("data")];
    let aggr_expr = vec![sum(col("data"))];

    let runtime = Runtime::new().expect("Failed to create Tokio runtime");

    let df = df.aggregate(group_expr, aggr_expr).unwrap();
    let mut results = runtime.block_on(df.collect()).unwrap();
    let batch = results.remove(0);
    let sum = batch
        .column(0)
        .as_any()
        .downcast_ref::<arcon::UInt64Array>()
        .unwrap();

    Ok(sum.value(0))
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
        .collection(
            (0..100000)
                .map(|i| Event { data: i })
                .collect::<Vec<Event>>(),
            |conf| {
                conf.set_arcon_time(ArconTime::Process);
            },
        )
        .window(WindowBuilder {
            assigner: Assigner::Tumbling {
                length: Time::seconds(2000),
                late_arrival: Time::seconds(0),
            },
            function: Arc::new(|backend: Arc<Sled>| ArrowWindow::new(backend, &arrow_udf)),
            conf: Default::default(),
        })
        .to_console()
        .build();

    pipeline.start();
    pipeline.await_termination();
}
