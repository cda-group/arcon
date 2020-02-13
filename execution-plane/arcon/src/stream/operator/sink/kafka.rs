// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::prelude::*;
use futures::executor::block_on;
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaResult;
use rdkafka::producer::{FutureProducer, FutureRecord};

/*
    KafkaSink: Buffers received elements
    Writes and commits buffers on epoch
*/
#[allow(dead_code)]
pub struct KafkaSink<IN>
where
    IN: ArconType + ::serde::Serialize + ::serde::de::DeserializeOwned,
{
    bootstrap_server: String,
    topic: String,
    offset: u32,
    batch_size: u32,
    producer: FutureProducer,
    buffer: Vec<ArconElement<IN>>,
}

impl<IN> KafkaSink<IN>
where
    IN: ArconType + ::serde::Serialize + ::serde::de::DeserializeOwned,
{
    pub fn new(bootstrap_server: String, topic: String, offset: u32) -> KafkaSink<IN> {
        let mut config = ClientConfig::new();
        config
            .set("group.id", "example_consumer_group_id")
            .set("bootstrap.servers", &bootstrap_server)
            .set("produce.offset.report", "true")
            .set("message.timeout.ms", "5000");
        let result: KafkaResult<FutureProducer> = config.create();
        match result {
            Ok(producer) => KafkaSink {
                bootstrap_server,
                topic,
                offset,
                batch_size: 100,
                producer,
                buffer: Vec::new(),
            },
            _ => {
                panic!("Failed to start KafkaSink");
            }
        }
    }

    pub fn commit_buffer(&mut self) -> () {
        //println!("sink committing buffer");
        // Will asynchronously try to write all messages in the buffer
        // But will block the thread until all commits are complete
        let mut futures = Vec::new();
        for element in self.buffer.drain(..) {
            if let Ok(serialized) = serde_json::to_string(&element.data) {
                futures.push(self.producer.send(
                    FutureRecord::to(&self.topic).payload(&serialized).key(&()),
                    0, // The future will return RDKafkaError::QueueFull without retrying
                ));
            }
        }

        // Write synchronously
        for future in futures {
            let _ = block_on(future);
        }
    }
}

impl<IN> Operator<IN, IN> for KafkaSink<IN>
where
    IN: ArconType + ::serde::Serialize + ::serde::de::DeserializeOwned,
{
    fn handle_element(&mut self, element: ArconElement<IN>) -> Option<Vec<ArconEvent<IN>>> {
        self.buffer.push(element);
        None
    }
    fn handle_watermark(&mut self, _w: Watermark) -> Option<Vec<ArconEvent<IN>>> {
        None
    }
    fn handle_epoch(&mut self, _epoch: Epoch) -> Option<ArconResult<Vec<u8>>> {
        self.commit_buffer();
        None
    }
}

// Tested via kafka_source
