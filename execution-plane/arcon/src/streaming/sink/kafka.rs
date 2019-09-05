use crate::prelude::*;
use kompact::*;
use std::str::FromStr;
use std::time::Duration;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::message::*;
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaResult, KafkaError, RDKafkaError};
use serde::{Deserialize};
use futures::{Future, Complete};
/*
    KafkaSink:
        Will subscribe
*/
#[derive(ComponentDefinition)]
pub struct KafkaSink<IN>
where
    IN: 'static + ArconType,
{
    ctx: ComponentContext<KafkaSink<IN>>,
    bootstrap_server: String,
    topic: String,
    offset: u32,
    batch_size: u32,
    producer: FutureProducer,
    buffer: Vec<ArconElement<IN>>,
}

impl<IN> KafkaSink<IN>
where
    IN: 'static + ArconType,
{
    pub fn new(
        bootstrap_server: String,
        topic: String,
        offset: u32,
    ) -> KafkaSink<IN> {
        let mut config = ClientConfig::new();
        config.set("group.id", "example_consumer_group_id")
        .set("bootstrap.servers", &bootstrap_server)
        .set("produce.offset.report", "true")
        .set("message.timeout.ms", "5000");
        let result: KafkaResult<FutureProducer> = config.create();
        match result {
            Ok(producer) => {
                KafkaSink {
                    ctx: ComponentContext::new(),
                    bootstrap_server,
                    topic,
                    offset,
                    //max_timestamp: 0,
                    batch_size: 100,
                    producer: producer,
                    buffer: Vec::new(),
                }
            }
            _ => {
                panic!("Failed to start KafkaSink");
            }
        }
    }
    pub fn handle_event(&mut self, event: ArconEvent<IN>) -> () {
        match event {
            ArconEvent::Element(e) => {
                // Buffer the element
                self.buffer.push(e);
            }
            ArconEvent::Watermark(w) => {
                // Do nothing
            }
        }
        // Todo: strategy for commits
        self.commit_buffer();
    }

    pub fn commit_buffer(&mut self) -> () {
        // Will asynchronously try to write all messages in the buffer
        // But will block the thread until all commits are complete
        let mut futures = Vec::new();
        for element in self.buffer.drain(..) {
            if let Ok(serialized) = serde_json::to_string(&element.data) {
                futures.push(self.producer.send(
                    FutureRecord::to(&self.topic)
                        .payload(&serialized)
                        .key(&()),
                        0 // The future will return RDKafkaError::QueueFull without retrying
                    )
                );
            }
        }
        //let mut new_buffer = Vec::new();
        for future in futures {
            match future.wait() {
                Complete => {
                }    
            } 

        }
    }
}

impl<IN> Provide<ControlPort> for KafkaSink<IN>
where
    IN: 'static + ArconType,
{
    fn handle(&mut self, event: ControlEvent) -> () {
        match event {
            ControlEvent::Start => {
                
            }
            _ => {
                error!(self.ctx.log(), "bad ControlEvent");
            }
        }
    }
}

impl<IN> Actor for KafkaSink<IN>
where
    IN: 'static + ArconType,
{
    fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
        if let Some(event) = msg.downcast_ref::<ArconEvent<IN>>() {
            self.handle_event(*event);
        }
    }
    fn receive_message(&mut self, _sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{thread, time};
    #[arcon]
    struct Thing {
        id: u32,
        attribute: i32,
        location: Point,
    }
    #[arcon]
    struct Point {
        x: f32,
        y: f32,
    }

    //#[test] Used for "manual testing" during developement
    fn kafka_sink() -> Result<()> { 
        let system = KompactConfig::default().build().expect("KompactSystem");

        let kafka_source: KafkaSink<Thing> = KafkaSink::new(
            "localhost:9092".to_string(), 
            "test".to_string(),
            0,
        );
        let (source, _) = system.create_and_register(move || kafka_source);

        system.start(&source);
        let thing_a = Thing{id: 0, attribute: 100, location: Point{x: 0.52, y: 113.3233}};
        let thing_b = Thing{id: 1, attribute: 101, location: Point{x: -0.52, y: 15.0}};
        source.actor_ref().tell(Box::new(ArconEvent::Element(ArconElement::new(thing_a))), &system);
        source.actor_ref().tell(Box::new(ArconEvent::Element(ArconElement::new(thing_b))), &system);
        thread::sleep(time::Duration::from_secs(10));
        Ok(())
    }
}