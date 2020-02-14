// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::data::{ArconElement, ArconEvent, ArconType, Watermark};
use crate::prelude::KompactSystem;
use crate::stream::channel::strategy::ChannelStrategy;
use crate::stream::operator::Operator;
use crate::util::SafelySendableFn;

pub mod collection;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod local_file;
#[cfg(feature = "socket")]
pub mod socket;

/// Common Context for all Source implementations
pub struct SourceContext<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    /// Maximum buffer size before flushing
    buffer_limit: u64,
    /// Counter keeping track of number events buffered
    buffer_counter: u64,
    /// Timestamp extractor function
    ts_extractor: Option<&'static dyn for<'r> SafelySendableFn<(&'r IN,), u64>>,
    /// Current Watermark
    current_watermark: u64,
    /// Watermark interval
    ///
    /// Controls how often the source generates watermarks. For finite
    /// sources, `watermark_interval` may be an element counter. Whereas
    /// in an unbounded source type, it may be the timer timeout period.
    pub watermark_interval: u64,
    /// An Operator to enable fusion of computation within the source
    operator: Box<dyn Operator<IN, OUT> + Send>,
    /// Strategy for outputting events
    channel_strategy: ChannelStrategy<OUT>,
}

impl<IN, OUT> SourceContext<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    pub fn new(
        buffer_limit: u64,
        watermark_interval: u64,
        ts_extractor: Option<&'static dyn for<'r> SafelySendableFn<(&'r IN,), u64>>,
        channel_strategy: ChannelStrategy<OUT>,
        operator: Box<dyn Operator<IN, OUT> + Send>,
    ) -> Self {
        SourceContext {
            buffer_limit,
            buffer_counter: 0,
            ts_extractor,
            current_watermark: 0,
            watermark_interval,
            operator,
            channel_strategy,
        }
    }

    /// Generates a `Watermark` event and sends it downstream
    pub fn generate_watermark(&mut self, source: &KompactSystem) {
        let wm_event: ArconEvent<OUT> = {
            if self.has_timestamp_extractor() {
                ArconEvent::Watermark(Watermark::new(self.current_watermark))
            } else {
                let system_time = crate::util::get_system_time();
                self.watermark_update(system_time);
                ArconEvent::Watermark(Watermark::new(self.current_watermark))
            }
        };

        self.output_event(wm_event, source);
    }

    /// Helper to know whether to use SystemTime or EventTime
    #[inline]
    fn has_timestamp_extractor(&self) -> bool {
        self.ts_extractor.is_some()
    }

    /// Update Watermark if `ts` is of a higher value than the current Watermark
    #[inline]
    pub fn watermark_update(&mut self, ts: u64) {
        if ts > self.current_watermark {
            self.current_watermark = ts;
        }
    }

    /// Calls a transformation function on the source data to generate outgoing ArconEvent<OUT>
    #[inline]
    pub fn process(&mut self, data: ArconElement<IN>, source: &KompactSystem) {
        if let Some(events) = self.operator.handle_element(data) {
            for event in events {
                self.output_event(event, source);
            }
        }
    }

    /// Internal helper function that handles batching and sending of events
    #[inline]
    fn output_event(&mut self, event: ArconEvent<OUT>, source: &KompactSystem) {
        if let ArconEvent::Element(_) = &event {
            self.channel_strategy.add(event);
            self.buffer_counter += 1;
            if self.buffer_counter == self.buffer_limit {
                self.channel_strategy.flush(source);
                self.buffer_counter = 0;
            }
        } else {
            // Send Epoch/Watermark marker downstream as soon as possible.
            self.channel_strategy.add_and_flush(event, source);
            self.buffer_counter = 0;
        }
    }

    /// Build ArconElement
    ///
    /// Extracts timestamp if extractor is available
    #[inline]
    pub fn extract_element(&mut self, data: IN) -> ArconElement<IN> {
        match &self.ts_extractor {
            Some(ts_fn) => {
                let ts = (ts_fn)(&data);
                self.watermark_update(ts);
                ArconElement::with_timestamp(data, ts)
            }
            None => ArconElement::new(data),
        }
    }
}
