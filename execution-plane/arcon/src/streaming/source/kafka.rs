use serde::de::DeserializeOwned;
use crate::prelude::*;
use kompact::*;
use std::str::FromStr;
use std::time::Duration;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer};
use rdkafka::message::*;
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaResult, KafkaError};
use serde::{Deserialize};

/*
    KafkaSource:
        Will subscribe
*/
#[derive(ComponentDefinition)]
pub struct KafkaSource<OUT>
where
    OUT: 'static + ArconType + DeserializeOwned,
{
    ctx: ComponentContext<KafkaSource<OUT>>,
    out_channels: Box<ChannelStrategy<OUT>>,
    bootstrap_server: String,
    topic: String,
    offset: u32,
    max_timestamp: u64,
    batch_size: u32,
    consumer: StreamConsumer,
}

impl<OUT> KafkaSource<OUT>
where
    OUT: 'static + ArconType + DeserializeOwned,
{
    pub fn new(
        out_channels: Box<ChannelStrategy<OUT>>,
        bootstrap_server: String,
        topic: String,
        offset: u32,
    ) -> KafkaSource<OUT> {
        let mut config = ClientConfig::new();
        config.set("group.id", "example_consumer_group_id")
        .set("bootstrap.servers", &bootstrap_server)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false");
        let result: KafkaResult<StreamConsumer> = config.create();
        match result {
            Ok(consumer) => {
                if let Err(e) = consumer.subscribe(&[&topic]) {
                    panic!("KafkaSource unable to subscribe to topic {}\nerror:{:?}", topic, e);
                }
                //let stream = consumer.start_with(Duration::from_millis(100), true);
                KafkaSource {
                    ctx: ComponentContext::new(),
                    out_channels,
                    bootstrap_server,
                    topic,
                    offset,
                    max_timestamp: 0,
                    batch_size: 100,
                    consumer: consumer,
                }
            }
            _ => {
                panic!("Failed to start KafkaSource");
            }
        }
    }
    pub fn output_event(&mut self, data: OUT, timestamp: Option<u64>) -> () {
        if let Err(err) = self.out_channels.output(
            ArconEvent::Element(ArconElement{data, timestamp}),
            &self.ctx.system()) {
                error!(self.ctx.log(), "Unable to output Element, error {}", err);
        }
    }
    pub fn output_watermark(&mut self) -> () {
        let ts = self.max_timestamp;
        if let Err(err) = self.out_channels.output(
            ArconEvent::Watermark(Watermark{timestamp: ts}),
            &self.ctx.system()) {
                error!(self.ctx.log(), "Unable to output watermark, error {}", err);
        }
    }

    pub fn receive(&mut self) -> () {
        let mut messages = Vec::new();
        let stream = self.consumer.start_with(Duration::from_millis(100), true);
        let mut counter = 0;
        // Fetch the batch
        for message in stream.wait() {
            match message {
                Ok(Ok(m)) => messages.push(m.detach()),
                Ok(Err(KafkaError::NoMessageReceived)) => break, // No more messages pending
                Err(_) => error!(self.ctx.log(), "Error while reading from stream."),
                Ok(Err(e)) => error!(self.ctx.log(), "Kafka error: {}", e),
            };
            counter = counter + 1;
            if counter == self.batch_size {
                break;
            }
        }
        // Process the batch
        for m in messages {
            let payload = match m.payload_view::<str>() {
                None => "",
                Some(Ok(s)) => s,
                Some(Err(e)) => {
                    error!(self.ctx.log(), "Error while deserializing message payload: {:?}", e);
                    ""
                },
            }; /*
            let key = match m.key_view::<str>() {
                None => "",
                Some(Ok(s)) => s,
                Some(Err(e)) => {
                    error!(self.ctx.log(), "Error while deserializing message key: {:?}", e);
                    ""
                },
            }; */
            let mut timestamp: Option<u64> = None;
            if let Some(ts) = m.timestamp().to_millis() {
                timestamp = Some(ts as u64);
                if ts as u64 > self.max_timestamp {
                    self.max_timestamp = ts as u64;
                }
            }
            if let Ok(data) = serde_json::from_str(&payload) {
                self.output_event(data, timestamp);
            } else {
                error!(self.ctx.log(), "Unable to deserialize message:\nkey: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                      m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
            }
            /*
            if let Some(headers) = m.headers() {
                for i in 0..headers.count() {
                    let header = headers.get(i).unwrap();
                    println!("  Header {:#?}: {:?}", header.0, header.1);
                }
            }
            // todo (somewhere else) 
            consumer.commit_message(&m, CommitMode::Async).unwrap(); auto-committing
            */
        }
        if counter > 0 {
            self.output_watermark();
            // Store Offset manage epoch here.
        }
        // Schedule next batch
        self.schedule_once(Duration::from_millis(1000), move |self_c, _|{self_c.receive()});
    }
}

impl<OUT> Provide<ControlPort> for KafkaSource<OUT>
where
    OUT: 'static + ArconType + DeserializeOwned,
{
    fn handle(&mut self, event: ControlEvent) -> () {
        match event {
            ControlEvent::Start => {
                self.receive();
            }
            _ => {
                error!(self.ctx.log(), "bad ControlEvent");
            }
        }
    }
}

impl<OUT> Actor for KafkaSource<OUT>
where
    OUT: 'static + ArconType + DeserializeOwned,
{
    fn receive_local(&mut self, _sender: ActorRef, _msg: &Any) {
    }
    fn receive_message(&mut self, _sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{thread, time};
    // Stub for window-results
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
    // JSON Example: {"id":1, "attribute":-13,"location":{"x":0.14124,"y":5882.231}}
    mod sink {
        use core::fmt::Debug;
        use super::*;
        use std::sync::Arc;

        pub struct Sink<A: 'static + ArconType + Debug> {
            ctx: ComponentContext<Sink<A>>,
            pub result: Vec<ArconElement<A>>,
            pub watermarks: Vec<Watermark>,
        }
        impl<A: ArconType + Debug> Sink<A> {
            pub fn new() -> Sink<A> {
                Sink {
                    ctx: ComponentContext::new(),
                    result: Vec::new(),
                    watermarks: Vec::new(),
                }
            }
        }
        impl<A: ArconType + Debug> Provide<ControlPort> for Sink<A> {
            fn handle(&mut self, _event: ControlEvent) -> () {}
        }
        impl<A: ArconType + Debug> Actor for Sink<A> {
            fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
                if let Some(event) = msg.downcast_ref::<ArconEvent<A>>() {
                    match event {
                        ArconEvent::Element(e) => {
                            println!("Sink receieved element: {:?} // {:?}", e.data, e.timestamp);
                            self.result.push(*e);
                        }
                        ArconEvent::Watermark(w) => {
                            println!("Sink receieved watermark: {}", w.timestamp);
                            self.watermarks.push(*w);
                        }
                    }
                } else {
                }
            }
            fn receive_message(&mut self, _sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {}
        }
        impl<A: ArconType + Debug> ComponentDefinition for Sink<A> {
            fn setup(&mut self, self_component: Arc<Component<Self>>) -> () {
                self.ctx_mut().initialise(self_component);
            }
            fn execute(&mut self, _max_events: usize, skip: usize) -> ExecuteResult {
                ExecuteResult::new(skip, skip)
            }
            fn ctx(&self) -> &ComponentContext<Self> {
                &self.ctx
            }
            fn ctx_mut(&mut self) -> &mut ComponentContext<Self> {
                &mut self.ctx
            }
            fn type_name() -> &'static str {
                "EventTimeWindowAssigner"
            }
        }
    }

    #[test]
    fn kafka_source() -> Result<()> { 
        let system = KompactConfig::default().build().expect("KompactSystem");

        let (sink, _) = system.create_and_register(move || sink::Sink::<Thing>::new());
        let sink_ref = sink.actor_ref();

        let out_channels: Box<Forward<Thing>> =
            Box::new(Forward::new(Channel::Local(sink_ref.clone())));

        let kafka_source: KafkaSource<Thing> = KafkaSource::new(
            out_channels, 
            "localhost:9092".to_string(), 
            "test".to_string(),
            0,
        );
        let (source, _) = system.create_and_register(move || kafka_source);

        system.start(&sink);
        system.start(&source);
        thread::sleep(time::Duration::from_secs(10));
        Ok(())
    }
}

