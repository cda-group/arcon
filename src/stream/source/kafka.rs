// Copyright (c) 2021, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{schema::SourceSchema, Poll, Source};
use crate::{
    error::source::{SourceError, SourceResult},
    index::{IndexOps, LazyValue, StateConstructor, ValueIndex},
};
use arcon_macros::ArconState;
use arcon_state::Backend;
use rdkafka::{
    config::{ClientConfig, FromClientConfig},
    consumer::{BaseConsumer, Consumer, DefaultConsumerContext},
    message::*,
    topic_partition_list::{Offset, TopicPartitionList},
};
use std::{sync::Arc, time::Duration};

/// Default timeout duration for consumer polling
const DEFAULT_POLL_TIMEOUT_MS: u64 = 250;

impl Default for KafkaConsumerConf {
    fn default() -> Self {
        Self {
            client_config: ClientConfig::default(),
            poll_timeout_ms: DEFAULT_POLL_TIMEOUT_MS,
            topic: None,
        }
    }
}
/// Kafka Source Configuration
#[derive(Debug, Clone)]
pub struct KafkaConsumerConf {
    /// Holds the config of [rdkafka] client
    client_config: ClientConfig,
    /// Timeout in milliseconds of how long to wait for during poll
    poll_timeout_ms: u64,
    /// Topic of interest
    topic: Option<String>,
}

impl KafkaConsumerConf {
    /// Set topic for the conf
    pub fn with_topic(mut self, topic: &str) -> Self {
        self.topic = Some(topic.to_string());
        self
    }
    /// Set poll timeout for the Kafka consumer
    ///
    /// If not defined, the default [DEFAULT_POLL_TIMEOUT] will be used.
    pub fn with_poll_timeout(mut self, timeout_ms: u64) -> Self {
        self.poll_timeout_ms = timeout_ms;
        self
    }

    /// Configure rdkafka's ClientConfig
    pub fn set(mut self, key: &str, value: &str) -> Self {
        self.client_config.set(key, value);
        self
    }

    pub fn client_config(&self) -> &ClientConfig {
        &self.client_config
    }
    pub fn topic(&self) -> &str {
        &self.topic.as_ref().unwrap()
    }
    pub fn poll_timeout(&self) -> u64 {
        self.poll_timeout_ms
    }
}

#[derive(ArconState)]
pub struct KafkaConsumerState<B: Backend> {
    partition_offsets: LazyValue<i64, B>,
    epoch_offsets: LazyValue<i64, B>,
}

impl<B: Backend> StateConstructor for KafkaConsumerState<B> {
    type BackendType = B;
    fn new(backend: Arc<B>) -> Self {
        Self {
            partition_offsets: LazyValue::new("_partition_offsets", backend.clone()),
            epoch_offsets: LazyValue::new("_epoch_offsets", backend),
        }
    }
}

/// A Parallel Kafka Source
///
/// A single instance may be responsible for one or more partitions.
pub struct KafkaConsumer<S, B>
where
    S: SourceSchema,
    B: Backend,
{
    conf: KafkaConsumerConf,
    consumer: BaseConsumer<DefaultConsumerContext>,
    state: KafkaConsumerState<B>,
    schema: S,
}

impl<S, B> KafkaConsumer<S, B>
where
    S: SourceSchema,
    B: Backend,
{
    pub fn new(
        conf: KafkaConsumerConf,
        mut state: KafkaConsumerState<B>,
        schema: S,
        source_index: usize,
        total_sources: usize,
    ) -> Self {
        let consumer = BaseConsumer::from_config(&conf.client_config()).unwrap();

        let metadata = consumer
            .fetch_metadata(Some(conf.topic()), Duration::from_millis(6000))
            .expect("Failed to fetch metadata");

        let topic = metadata.topics().get(0).unwrap();
        let partitions = topic.partitions().len();
        assert!(
            partitions >= total_sources,
            "Total kafka partitions must be equal or greater than the number of sources"
        );
        // caclulate which partitions this instance should take care of
        let start = (source_index * partitions + total_sources - 1) / total_sources;
        let end = ((source_index + 1) * partitions - 1) / total_sources;

        let mut tpl = TopicPartitionList::new();
        for partition in start..end + 1 {
            state.partition_offsets().set_key(partition as u64);
            let offset = match state.partition_offsets().get() {
                Ok(Some(off)) => Offset::Offset(*off),
                _ => Offset::Beginning,
            };

            tpl.add_partition_offset(conf.topic(), partition as i32, offset)
                .unwrap();
        }
        consumer
            .assign(&tpl)
            .expect("failed to assign TopicParitionList");

        consumer
            .subscribe(&[conf.topic()])
            .expect("failed to subscribe to topic");

        Self {
            conf,
            consumer,
            state,
            schema,
        }
    }
}

impl<S, B> Source for KafkaConsumer<S, B>
where
    S: SourceSchema,
    B: Backend,
{
    type Item = S::Data;

    fn poll_next(&mut self) -> SourceResult<Poll<Self::Item>> {
        match self
            .consumer
            .poll(Duration::from_millis(self.conf.poll_timeout()))
        {
            Some(Ok(msg)) => match msg.payload() {
                Some(bytes) => match self.schema.from_bytes(bytes) {
                    Ok(data) => {
                        let partition = msg.partition();
                        let offset = msg.offset();
                        self.state.partition_offsets.set_key(partition as u64);
                        self.state.partition_offsets.put(offset)?;
                        Ok(Ok(Poll::Ready(data)))
                    }
                    Err(err) => Ok(Err(err)),
                },
                None => Ok(Ok(Poll::Pending)),
            },
            Some(Err(err)) => Ok(Err(SourceError::Kafka { error: err })),
            None => {
                // Nothing to collect
                Ok(Ok(Poll::Pending))
            }
        }
    }
    fn set_offset(&mut self, _: usize) {}
}
