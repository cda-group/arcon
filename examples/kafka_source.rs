#[arcon::app]
fn main() {
    let consumer_conf = KafkaConsumerConf::default()
        .with_topic("test")
        .set("group.id", "test")
        .set("bootstrap.servers", "localhost:9092")
        .set("enable.auto.commit", "false");

    let paralellism = 2;

    KafkaSource::new(consumer_conf, ProtoSchema::new(), paralellism)
        .to_stream(|conf| {
            conf.set_arcon_time(ArconTime::Event);
            conf.set_timestamp_extractor(|x: &u64| *x);
        })
        .map(|x| x + 10)
        .print()
}
