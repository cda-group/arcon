// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::*,
    stream::operator::{EventVec, OperatorContext},
    timer::TimerBackend,
};
use arcon_state::{RegistrationToken, Session};
use futures::executor::block_on;
use rdkafka::{
    config::ClientConfig,
    error::KafkaResult,
    producer::{FutureProducer, FutureRecord},
};
use std::cell::RefCell;

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
    buffer: RefCell<Vec<ArconElement<IN>>>,
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
                buffer: RefCell::new(Vec::new()),
            },
            _ => {
                panic!("Failed to start KafkaSink");
            }
        }
    }

    pub fn commit_buffer(&self) -> () {
        //println!("sink committing buffer");
        // Will asynchronously try to write all messages in the buffer
        // But will block the thread until all commits are complete
        let mut futures = Vec::new();
        for element in self.buffer.borrow_mut().drain(..) {
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

impl<IN, B: state::Backend> Operator<B> for KafkaSink<IN>
where
    IN: ArconType + ::serde::Serialize + ::serde::de::DeserializeOwned,
{
    type IN = IN;
    type OUT = ArconNever;
    type TimerState = ArconNever;

    fn register_states(&mut self, _registration_token: &mut RegistrationToken<B>) {
        ()
    }

    fn init(&mut self, _session: &mut Session<B>) {
        ()
    }

    fn handle_element(
        &self,
        element: ArconElement<IN>,
        _ctx: OperatorContext<Self, B, impl TimerBackend<Self::TimerState>>,
    ) -> EventVec<Self::OUT> {
        self.buffer.borrow_mut().push(element);
        smallvec![]
    }
    crate::ignore_watermark!(B);
    crate::ignore_timeout!(B);

    fn handle_epoch(
        &self,
        _epoch: Epoch,
        _ctx: OperatorContext<Self, B, impl TimerBackend<Self::TimerState>>,
    ) -> Option<EventVec<Self::OUT>> {
        self.commit_buffer();
        None
    }
}

// Tested via kafka_source
