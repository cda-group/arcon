// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only
use metrics::{
    counter, decrement_gauge, gauge, histogram, increment_counter, increment_gauge,
    register_counter, register_gauge, register_histogram, GaugeValue, Key, Recorder, Unit,
};

use crate::{
    conf::logger::ArconLogger,
    data::{ArconElement, ArconEvent, Epoch, Watermark},
    error::{source::SourceError, ArconResult},
    manager::source::{SourceManagerEvent, SourceManagerPort},
    metrics::runtime_metrics::{MetricValue, SourceNodeRuntimeMetrics},
    prelude::SourceConf,
    stream::{
        channel::strategy::ChannelStrategy,
        source::{Poll, Source},
        time::ArconTime,
    },
};
use kompact::prelude::*;
#[cfg(feature = "kafka")]
use rdkafka::error::KafkaError;
use std::cell::RefCell;

/// A message type that Source components in Arcon must implement
#[derive(Debug, PartialEq, Clone)]
pub enum SourceEvent {
    Epoch(Epoch),
    Watermark(ArconTime),
    Start,
}

#[derive(Debug, Clone, Copy)]
struct ProcessSource;
struct LoopbackPort;
impl Port for LoopbackPort {
    type Indication = Never;
    type Request = ProcessSource;
}

/// A [kompact] component to drive the execution of Arcon sources
#[derive(ComponentDefinition)]
pub struct SourceNode<S>
where
    S: Source,
{
    ctx: ComponentContext<Self>,
    manager_port: RequiredPort<SourceManagerPort>,
    loopback_send: RequiredPort<LoopbackPort>,
    loopback_receive: ProvidedPort<LoopbackPort>,
    watermark: u64,
    ended: bool,
    channel_strategy: RefCell<ChannelStrategy<S::Item>>,
    conf: SourceConf<S::Item>,
    source_index: usize,
    source: S,
    logger: ArconLogger,
    source_node_runtime_metrics: SourceNodeRuntimeMetrics,
    source_node_descriptor: String,
}

impl<S> SourceNode<S>
where
    S: Source,
{
    pub fn new(
        source_index: usize,
        source: S,
        conf: SourceConf<S::Item>,
        channel_strategy: ChannelStrategy<S::Item>,
        logger: ArconLogger,
    ) -> Self {
        let borrowed_source_name: &str = &conf.source_name.clone();
        Self {
            ctx: ComponentContext::uninitialised(),
            manager_port: RequiredPort::uninitialised(),
            loopback_send: RequiredPort::uninitialised(),
            loopback_receive: ProvidedPort::uninitialised(),
            channel_strategy: RefCell::new(channel_strategy),
            ended: false,
            watermark: 0,
            conf,
            source_index,
            source,
            logger,
            source_node_runtime_metrics: SourceNodeRuntimeMetrics::new(borrowed_source_name),
            source_node_descriptor: String::from(borrowed_source_name),
        }
    }
    pub fn process(&mut self) -> ArconResult<()> {
        let mut counter = 0;

        loop {
            self.source_node_runtime_metrics
                .incoming_message_rate
                .update_value(1);
            gauge!(
                [&self.source_node_descriptor, "_incoming_message_rate"].join("\n"),
                self.source_node_runtime_metrics
                    .incoming_message_rate
                    .get_value()
            );
            if counter >= self.conf.batch_size {
                return Ok(());
            }

            let poll = self.source.poll_next()?;

            match poll {
                Ok(Poll::Ready(record)) => {
                    match self.conf.time {
                        ArconTime::Event => match &self.conf.extractor {
                            Some(extractor) => {
                                let timestamp = extractor(&record);
                                self.output_with_timestamp(record, timestamp);
                            }
                            None => {
                                panic!("Cannot use ArconTime::Event without an timestamp extractor")
                            }
                        },
                        ArconTime::Process => self.output(record),
                    }
                    counter += 1;
                }
                Ok(Poll::Pending) => {
                    // nothing to collect, reschedule...
                    return Ok(());
                }
                Ok(Poll::Done) => {
                    // signal end..
                    self.ended = true;
                    return Ok(());
                }
                Err(error) => {
                    self.source_node_runtime_metrics
                        .error_counter
                        .update_value(1);
                    gauge!(
                        [&self.source_node_descriptor, "_error_counter"].join("\n"),
                        self.source_node_runtime_metrics
                            .incoming_message_rate
                            .get_value()
                    );
                    return self.handle_source_error(error);
                }
            }
        }
    }

    fn handle_source_error(&self, source_error: SourceError) -> ArconResult<()> {
        #[cfg(feature = "kafka")]
        if let SourceError::Kafka { error } = &source_error {
            match error {
                // TODO: figure out which other kafka errors should cause a stop
                KafkaError::Canceled | KafkaError::ConsumerCommit(_) => {
                    return Err(crate::error::Error::Unsupported {
                        msg: error.to_string(),
                    })
                }
                _ => (),
            }
        }

        // if we reach here, it means the error was not that serious...
        // but we log it
        error!(self.logger, "{}", source_error);
        Ok(())
    }

    #[inline]
    pub fn output(&mut self, data: S::Item) {
        self.send(ArconEvent::Element(ArconElement::new(data)));
    }

    #[inline]
    pub fn output_with_timestamp(&mut self, data: S::Item, timestamp: u64) {
        self.update_watermark(timestamp);
        self.send(ArconEvent::Element(ArconElement::with_timestamp(
            data, timestamp,
        )));
    }

    #[inline(always)]
    fn send(&mut self, event: ArconEvent<S::Item>) {
        self.channel_strategy.borrow_mut().add(event, self);
    }

    #[inline(always)]
    fn update_watermark(&mut self, ts: u64) {
        self.watermark = std::cmp::max(ts, self.watermark);
    }

    pub fn handle_source_event(&mut self, event: SourceEvent) {
        match event {
            SourceEvent::Epoch(epoch) => {
                self.send(ArconEvent::Epoch(epoch));
            }
            SourceEvent::Watermark(time) => {
                let wm = match time {
                    ArconTime::Event => Watermark::new(self.watermark),
                    ArconTime::Process => {
                        let system_time = crate::util::get_system_time();
                        Watermark::new(system_time)
                    }
                };

                // update internal watermark
                self.update_watermark(wm.timestamp);

                // send watermark downstream
                self.send(ArconEvent::Watermark(wm));
            }
            SourceEvent::Start => {
                self.loopback_send.trigger(ProcessSource);
            }
        }
    }
}

impl<S> ComponentLifecycle for SourceNode<S>
where
    S: Source,
{
    fn on_start(&mut self) -> Handled {
        info!(
            self.logger,
            "Starting up Source with Index {}", self.source_index
        );
        let shared = self.loopback_receive.share();
        self.loopback_send.connect(shared);
        Handled::Ok
    }
}

impl<S> Provide<LoopbackPort> for SourceNode<S>
where
    S: Source,
{
    fn handle(&mut self, _event: ProcessSource) -> Handled {
        if let Err(error) = self.process() {
            // fatal error, must shutdown..
            // TODO: coordinate shutdown of pipeline..
            error!(self.logger, "{}", error);
        }

        if self.ended {
            self.manager_port.trigger(SourceManagerEvent::End);
        } else {
            self.loopback_send.trigger(ProcessSource);
        }
        Handled::Ok
    }
}

impl<S> Require<LoopbackPort> for SourceNode<S>
where
    S: Source,
{
    fn handle(&mut self, _event: Never) -> Handled {
        unreachable!("Never type has no instance");
    }
}

impl<S> Require<SourceManagerPort> for SourceNode<S>
where
    S: Source,
{
    fn handle(&mut self, _event: Never) -> Handled {
        unreachable!("Never type has no instance");
    }
}

impl<S> Actor for SourceNode<S>
where
    S: Source,
{
    type Message = SourceEvent;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        self.handle_source_event(msg);
        Handled::Ok
    }
    fn receive_network(&mut self, _: NetMessage) -> Handled {
        Handled::Ok
    }
}
