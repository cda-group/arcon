use arcon::prelude::*;

fn main() {
    let consumer_conf = KafkaConsumerConf::default()
        .with_topic("test")
        .set("group.id", "test")
        .set("bootstrap.servers", "localhost:9092")
        .set("enable.auto.commit", "false");

    let mut app = Application::default()
        .kafka(consumer_conf, JsonSchema::new(), 2, |conf| {
            conf.set_arcon_time(ArconTime::Event);
            conf.set_timestamp_extractor(|x: &u64| *x);
        })
        .map(|x| x + 10)
        .to_console()
        .build();

    app.start();
    app.await_termination();
}
