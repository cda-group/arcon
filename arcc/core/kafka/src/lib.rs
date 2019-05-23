extern crate rdkafka;

use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;

pub mod prelude {
    pub use rdkafka::config::ClientConfig;
}

#[allow(dead_code)]
pub struct EventConsumer {
    consumer: StreamConsumer,
}


impl EventConsumer {
    pub fn new(topic: String, config: ClientConfig) -> Result<EventConsumer, Box<std::error::Error>>{
        let consumer: StreamConsumer = config.create()?;
        consumer.subscribe(&[&topic])?;
        Ok(EventConsumer {
            consumer
        })
    }
}


#[allow(dead_code)]
pub struct EventProducer {
    producer: FutureProducer,
    topic: String,
}

impl EventProducer {
    pub fn new(topic: String, config: ClientConfig) -> Result<EventProducer, Box<std::error::Error>> {
        let producer: FutureProducer = config.create()?;
        Ok(EventProducer {
            producer,
            topic,
        })
    }
}
