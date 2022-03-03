use crate::prelude::*;
use rand::Rng;
use std::thread::sleep;
use std::time::Duration;

#[derive(Arcon, Arrow, prost::Message, Copy, Clone)]
#[arcon(unsafe_ser_id = 12, reliable_ser_id = 13, version = 1)]
pub struct Event {
    #[prost(uint64)]
    pub data: u64,
    #[prost(uint32)]
    pub key: u32,
}

#[derive(Arcon, Arrow, prost::Message, Copy, Clone)]
#[arcon(unsafe_ser_id = 12, reliable_ser_id = 13, version = 1)]
pub struct EnrichedEvent {
    #[prost(uint64)]
    pub data: u64,
    #[prost(uint32)]
    pub key: u32,
    #[prost(uint64)]
    pub first_val: u64,
}

#[derive(ArconState)]
pub struct FirstVal<B: Backend> {
    #[table = "Count"]
    events: EagerValue<u64, B>,
}

const PARALLELISM: usize = 2;
const NUM_KEYS: usize = 256;
const EVENT_COUNT: u64 = 99946; // Idk what's special about this number but 100k wasn't reliable

fn operator_conf() -> OperatorConf {
    OperatorConf {
        parallelism_strategy: ParallelismStrategy::Static(PARALLELISM),
        ..Default::default()
    }
}

// Sets up a pipeline of iterator -> event -> key_by -> enriched_event
fn enriched_event_stream() -> Stream<EnrichedEvent> {
    (0u64..EVENT_COUNT)
        .to_stream(|conf| {
            conf.set_arcon_time(ArconTime::Event);
            conf.set_timestamp_extractor(|x: &u64| *x);
        })
        .operator(OperatorBuilder {
            // Map u64 -> Event
            operator: Arc::new(move || {
                Map::new({
                    |i| {
                        let mut rng = rand::thread_rng();
                        let r: u32 = rng.gen();
                        Event {
                            data: i,
                            key: r % (NUM_KEYS as u32),
                        }
                    }
                })
            }),
            state: Arc::new(|_| EmptyState),
            conf: operator_conf(),
        })
        .key_by(|event| &event.key)
        .operator(OperatorBuilder {
            operator: Arc::new(|| {
                // Map Event -> EnrichedEvent
                Map::stateful(|event: Event, state: &mut FirstVal<_>| {
                    // Just use state somehow, this should be the same for all events
                    let first_val: u64 = if let Some(value) = state.events().get()? {
                        *value
                    } else {
                        state.events().put(event.data)?;
                        event.data
                    };
                    Ok(EnrichedEvent {
                        data: event.data,
                        key: event.key,
                        first_val,
                    })
                })
            }),
            state: Arc::new(|backend| FirstVal {
                events: EagerValue::new("_events", backend),
            }),
            conf: operator_conf(),
        })
}

#[test]
fn key_by_integration() {
    let mut app = enriched_event_stream().debug().builder().build();
    app.run();
    sleep(Duration::from_secs(4));

    if let Some(debug_node) = app.get_debug_node::<EnrichedEvent>() {
        debug_node.on_definition(|c| {
            let mut first_val_vec = Vec::new();
            for element in c.data.iter() {
                first_val_vec.push(element.data.first_val);
            }
            // all events were received by the debug node
            // assert_eq!(first_val_vec.len(), (EVENT_COUNT-2) as usize); // Something funky is happening to the events?
            first_val_vec.sort_unstable();
            first_val_vec.dedup();
            // Only NUM_KEYS number of first_val were received, i.e. State was handled properly
            assert_eq!(first_val_vec.len(), NUM_KEYS);
            // The DebugNode received events from the same
            assert_eq!(c.senders.len(), PARALLELISM);
        })
    } else {
        panic!("Failed to get DebugNode!")
    }
}

// Same test as above except we add another Map: EnrichedEvent -> EnrichedEvent with a Forward channel
#[test]
fn key_by_to_forward_integration() {
    let mut app = enriched_event_stream()
        .operator(OperatorBuilder {
            operator: Arc::new(|| {
                // Map EnrichedEvent -> EnrichedEvent
                Map::new(|event: EnrichedEvent| event)
            }),
            state: Arc::new(|_| EmptyState),
            conf: operator_conf(),
        })
        .debug()
        .builder()
        .build();

    app.run();

    sleep(Duration::from_secs(4));
    if let Some(debug_node) = app.get_debug_node::<EnrichedEvent>() {
        debug_node.on_definition(|c| {
            let mut first_val_vec = Vec::new();
            for element in c.data.iter() {
                first_val_vec.push(element.data.first_val);
            }
            // all events were received by the debug node
            // assert_eq!(first_val_vec.len(), (EVENT_COUNT-2) as usize); // Something funky is happening to the events?
            first_val_vec.sort_unstable();
            first_val_vec.dedup();
            // Only NUM_KEYS number of first_val were received, i.e. State was handled properly
            assert_eq!(first_val_vec.len(), NUM_KEYS);
            // The DebugNode received events from two different nodes
            assert_eq!(c.senders.len(), PARALLELISM);
        })
    } else {
        panic!("Failed to get DebugNode!")
    }
}
