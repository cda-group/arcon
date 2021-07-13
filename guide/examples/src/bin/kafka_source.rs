use arcon::prelude::*;


fn main() {

    let mut perf_events = PerfEvents::new();
    perf_events.add(HardwareCounter::CPU_CYCLES);

    let consumer_conf = KafkaConsumerConf::default()
        .with_topic("test")
        .set("group.id", "test")
        .set("bootstrap.servers", "localhost:9092")
        .set("enable.auto.commit", "false");




    let mut pipeline = Pipeline::default()
        .kafka(consumer_conf, JsonSchema::new(), 1, |conf| {
            conf.set_arcon_time(ArconTime::Event);
            conf.set_timestamp_extractor(|x: &u64| *x);
        })
        .operator(OperatorBuilder {
            constructor: Arc::new(|_| Map::new(|x| x + 10)),
            conf: OperatorConf {
                // parallelism_strategy: ParallelismStrategy::Managed,
                perf_events,
                ..Default::default()
            },
        })
        .to_console()
        .build();

    pipeline.start();
    pipeline.await_termination();
}
