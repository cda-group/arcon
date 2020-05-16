// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::prelude::*;
use futures::executor::block_on_stream;
use rdkafka::{
    config::ClientConfig,
    consumer::{stream_consumer::StreamConsumer, CommitMode, Consumer},
    error::KafkaResult,
    message::*,
    topic_partition_list::Offset,
};
use std::{collections::HashMap, time::Duration};

/*
    KafkaSource: work in progress
*/
#[allow(dead_code)]
#[derive(ComponentDefinition)]
pub struct KafkaSource<OUT>
where
    OUT: ArconType + ::serde::Serialize + ::serde::de::DeserializeOwned,
{
    ctx: ComponentContext<Self>,
    channel_strategy: ChannelStrategy<OUT>,
    bootstrap_server: String,
    topic: String,
    offset: Offset,
    max_timestamp: u64,
    batch_size: u32,
    consumer: StreamConsumer,
    id: NodeID,
    epoch: u64,
    commit_epoch: Option<u64>,
    epoch_offset: HashMap<u64, Offset>, // Maps epochs to offsets
    increment_epoch: bool,
}

impl<OUT> KafkaSource<OUT>
where
    OUT: ArconType + ::serde::Serialize + ::serde::de::DeserializeOwned,
{
    pub fn new(
        channel_strategy: ChannelStrategy<OUT>,
        bootstrap_server: String,
        topic: String,
        offset: i64,
        id: NodeID,
    ) -> KafkaSource<OUT> {
        let mut config = ClientConfig::new();
        config
            .set("group.id", "example_consumer_group_id")
            .set("bootstrap.servers", &bootstrap_server)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false");
        let result: KafkaResult<StreamConsumer> = config.create();
        match result {
            Ok(consumer) => {
                if let Err(e) = consumer.subscribe(&[&topic]) {
                    panic!(
                        "KafkaSource unable to subscribe to topic {}\nerror:{:?}",
                        topic, e
                    );
                }
                KafkaSource {
                    ctx: ComponentContext::new(),
                    channel_strategy,
                    bootstrap_server,
                    topic,
                    offset: Offset::from_raw(offset),
                    max_timestamp: 0,
                    batch_size: 100,
                    consumer,
                    id,
                    epoch: 0,
                    commit_epoch: None,
                    epoch_offset: HashMap::new(),
                    increment_epoch: false,
                }
            }
            _ => {
                panic!("Failed to start KafkaSource");
            }
        }
    }
    pub fn output_event(&mut self, data: OUT, ts: Option<u64>) -> () {
        match ts {
            Some(time) => {
                self.channel_strategy
                    .add(ArconEvent::Element(ArconElement::with_timestamp(
                        data, time,
                    )))
            }
            None => self
                .channel_strategy
                .add(ArconEvent::Element(ArconElement::new(data))),
        }
    }
    pub fn output_watermark(&mut self) -> () {
        let ts = self.max_timestamp;
        self.channel_strategy
            .add(ArconEvent::Watermark(Watermark::new(ts)));
    }
    pub fn commit_epoch(&mut self, epoch: &u64) -> () {
        if let Some(commit_offset) = self.epoch_offset.get(epoch) {
            if let Ok(borrowed_tpl) = self.consumer.assignment() {
                let mut tpl = borrowed_tpl.clone();
                //println!("committing offset: {}", commit_offset.to_raw());
                tpl.set_all_offsets(*commit_offset);
                if let Err(err) = self.consumer.commit(&tpl, CommitMode::Sync) {
                    error!(self.ctx.log(), "Failed to commit offset: {}", err);
                }
            } else {
                error!(
                    self.ctx.log(),
                    "Failed to get consumer assignment when committing"
                );
            }
        } else {
            error!(
                self.ctx.log(),
                "Unable to commit epoch, no corresponding Offset stored"
            );
        }
    }
    pub fn new_epoch(&mut self) -> () {
        self.epoch_offset.insert(self.epoch, self.offset);
        self.channel_strategy
            .add(ArconEvent::Epoch(Epoch::new(self.epoch)));
        self.epoch = self.epoch + 1;
    }

    pub fn receive(&mut self) -> () {
        let mut messages = Vec::new();
        let stream = self.consumer.start_with(Duration::from_millis(100), true);
        let mut counter = 0;

        // Fetch the batch
        for message in block_on_stream(stream) {
            match message {
                Ok(m) => messages.push(m.detach()),
                Err(_) => error!(self.ctx.log(), "Error while reading from stream."),
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
                    error!(
                        self.ctx.log(),
                        "Error while deserializing message payload: {:?}", e
                    );
                    ""
                }
            };
            /*
            // Ignoring key and header stuff for now
            let key = match m.key_view::<str>() {
                None => "",
                Some(Ok(s)) => s,
                Some(Err(e)) => {
                   error!(self.ctx.log(), "Error while deserializing message key: {:?}", e);
                   ""
                },
            };
            if let Some(headers) = m.headers() {
                for i in 0..headers.count() {
                    let header = headers.get(i).unwrap();
                    println!("  Header {:#?}: {:?}", header.0, header.1);
                }
            }
            */
            let mut timestamp: Option<u64> = None;
            if let Some(ts) = m.timestamp().to_millis() {
                timestamp = Some(ts as u64);
                if ts as u64 > self.max_timestamp {
                    self.max_timestamp = ts as u64;
                }
            }
            if let Ok(data) = serde_json::from_str(&payload) {
                //println!("source outputting {}, offset {}", &payload, m.offset());
                self.offset = Offset::from_raw(m.offset() + 1); // Store the offset+1 locally
                self.output_event(data, timestamp);
            } else {
                error!(self.ctx.log(), "Unable to deserialize message:\nkey: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                      m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
            }
        }
        // Schedule next batch
        self.schedule_once(Duration::from_millis(1000), move |self_c, _| {
            self_c.receive()
        });

        // Output watermark and manage Epochs.
        if counter > 0 {
            self.output_watermark();
            // We do one epoch per batch for now
            self.new_epoch();
            // We commit epochs when initializing new epoch for now
            self.commit_epoch(&(self.epoch - 1));
        }
    }
}

impl<OUT> Provide<ControlPort> for KafkaSource<OUT>
where
    OUT: ArconType + ::serde::Serialize + ::serde::de::DeserializeOwned,
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
    OUT: ArconType + ::serde::Serialize + ::serde::de::DeserializeOwned,
{
    type Message = Box<dyn Any + Send>;
    fn receive_local(&mut self, _msg: Self::Message) {}
    fn receive_network(&mut self, _msg: NetMessage) {}
}

// (Max) Disabling this until further notice
/*
#[cfg(test)]
mod tests {
    use super::*;
    use crate::macros::*;
    use crate::prelude::{DebugNode, KafkaSink};
    use crate::stream::util::mute_strategy;
    use std::{thread, time};

    #[arcon]
    pub struct Thing {
        #[prost(uint32, tag = "1")]
        pub id: u32,
        #[prost(int32, tag = "2")]
        pub attribute: i32,
    }

    #[arcon]
    #[derive(prost::Message, ::serde::Serialize, ::serde::Deserialize)]
    struct Point {
        #[prost(message, tag = "1")]
        x: ArconF32,
        #[prost(message, tag = "2")]
        y: ArconF32,
    }
    // JSON Example: {"id":1, "attribute":-13,"location":{"x":0.14124,"y":5882.231}}

    // Run with cargo test kafka --features kafka
    // requires local instance of kafka running on port 9092 with topic "test" created
    #[test]
    #[should_panic]
    fn kafka_source() {
        // Boot up a sink which will write 2 Things to kafka
        kafka_sink();
        let system = KompactConfig::default().build().expect("KompactSystem");

        let (sink, _) = system.create_and_register(move || DebugNode::<Thing>::new());
        let sink_ref = sink.actor_ref().hold().expect("failed to fetch strong ref");

        let channel_strategy = ChannelStrategy::Forward(Forward::new(
            Channel::Local(sink_ref.clone()),
            NodeID::new(0),
        ));

        let kafka_source: KafkaSource<Thing> = KafkaSource::new(
            channel_strategy,
            "localhost:9092".to_string(),
            "test".to_string(),
            0,
            1.into(),
        );
        let (source, _) = system.create_and_register(move || kafka_source);

        system.start(&sink);
        system.start(&source);
        thread::sleep(time::Duration::from_secs(15));

        let sink_inspect = sink.definition().lock().unwrap();
        // thing_a and thing_b should've been received from KafkaSink test
        assert_eq!(sink_inspect.data[0].data.id, 0u32); // thing 1
        assert_eq!(sink_inspect.data[0].data.attribute, 100i32);
        assert_eq!(sink_inspect.data[1].data.id, 1u32); // thing 2
        assert_eq!(sink_inspect.data[1].data.attribute, 101i32);
    }

    fn kafka_sink() -> () {
        let system = KompactConfig::default().build().expect("KompactSystem");

        let kafka_sink: KafkaSink<Thing> =
            KafkaSink::new("localhost:9092".to_string(), "test".to_string(), 0);
        let (sink, _) = system.create_and_register(move || {
            Node::<Thing, Thing>::new(
                1.into(),
                vec![0.into()],
                mute_strategy::<Thing>(),
                Box::new(kafka_sink),
                InMemory::create("test".as_ref()).unwrap(),
            )
        });

        system.start(&sink);
        let thing_1 = Thing {
            id: 0,
            attribute: 100,
        };
        let thing_2 = Thing {
            id: 1,
            attribute: 101,
        };
        sink.actor_ref()
            .tell(ArconMessage::element(thing_1, Some(10), 0.into()));
        sink.actor_ref()
            .tell(ArconMessage::element(thing_2, Some(11), 0.into()));
        sink.actor_ref()
            .tell(ArconMessage::<Thing>::epoch(1, 0.into()));
    }
}
*/
