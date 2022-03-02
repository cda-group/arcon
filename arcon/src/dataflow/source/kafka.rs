use super::super::stream::Stream;
use super::ToStreamExt;
use crate::dataflow::builder::ParallelSourceBuilder;
use crate::dataflow::source::SourceBuilderType;
use crate::dataflow::{conf::DefaultBackend, conf::SourceConf};
use crate::prelude::KafkaConsumerConf;
use crate::stream::source::{
    kafka::{KafkaConsumer, KafkaConsumerState},
    schema::SourceSchema,
};
use std::sync::Arc;

/// An unbounded Kafka Source
///
/// Returns a [`Stream`] object that users may execute transformations on.
///
/// # Example
/// ```no_run
/// use arcon::prelude::*;
///
/// let consumer_conf = KafkaConsumerConf::default()
/// .with_topic("test")
/// .set("group.id", "test")
/// .set("bootstrap.servers", "localhost:9092")
/// .set("enable.auto.commit", "false");
///
/// let paralellism = 2;
/// KafkaSource::new(consumer_conf, ProtoSchema::new(), paralellism)
///     .to_stream(|conf| {
///         conf.set_arcon_time(ArconTime::Event);
///         conf.set_timestamp_extractor(|x: &u64| *x);
///     })
/// ```
pub struct KafkaSource<S: SourceSchema> {
    kafka_conf: KafkaConsumerConf,
    schema: S,
    parallelism: usize,
}

impl<S: SourceSchema> KafkaSource<S> {
    pub fn new(kafka_conf: KafkaConsumerConf, schema: S, parallelism: usize) -> Self {
        Self {
            kafka_conf,
            schema,
            parallelism,
        }
    }
}

impl<S: SourceSchema> ToStreamExt<S::Data> for KafkaSource<S> {
    fn to_stream<F: FnOnce(&mut SourceConf<S::Data>)>(self, f: F) -> Stream<S::Data> {
        let mut conf = SourceConf::default();
        f(&mut conf);

        let kafka_conf = self.kafka_conf;
        let schema = self.schema;
        let parallelism = self.parallelism;

        let builder = ParallelSourceBuilder {
            constructor: Arc::new(move |backend: Arc<DefaultBackend>, index, total_sources| {
                KafkaConsumer::new(
                    kafka_conf.clone(),
                    KafkaConsumerState::new(backend),
                    schema.clone(),
                    index,
                    total_sources,
                )
            }),
            conf,
            parallelism,
        };
        super::source_to_stream(SourceBuilderType::Parallel(builder))
    }
}
