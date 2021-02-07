use arcon::prelude::*;
use arrow::record_batch::RecordBatch;
use examples::SnapshotComponent;
use std::{path::Path, sync::Arc};

use arrow::util::pretty;

use datafusion::{datasource::MemTable, prelude::*};

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
    counter: LazyValue<u64, B>,
    events: EagerAppender<Event, B>,
}

impl<B: Backend> MyState<B> {
    pub fn new(backend: Arc<B>) -> Self {
        MyState {
            counter: LazyValue::new("_counter", backend.clone()),
            events: EagerAppender::new("_events", backend),
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conf = ArconConf {
        epoch_interval: 10000,
        ..Default::default()
    };
    let event = Event { id: 1, data: 1.0 };
    let mut table = event.to_arrow_table(2);
    let events = vec![Event { id: 1, data: 1.7 }, Event { id: 2, data: 2.5 }];
    table.load(events)?;
    assert_eq!(table.len(), 2);
    let arr = table.finish();

    let schema = Arc::new(event.schema());
    let batch = RecordBatch::try_new(schema.clone(), vec![
        arr.column(0).clone(),
        arr.column(1).clone(),
    ])
    .unwrap();

    let mut ctx = ExecutionContext::new();

    let provider = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("t", Box::new(provider));
    let df = ctx.table("t").unwrap();

    let filter = col("id").eq(lit(1));

    let df = df
        .select_columns(vec!["id", "data"])
        .unwrap()
        .filter(filter)
        .unwrap();
    let results = df.collect().await?;
    pretty::print_batches(&results)?;

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
                    state.counter().rmw(|v| *v += 1)?;
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

    Ok(())
}
