// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconElement, ArconEvent, Epoch, Watermark},
    prelude::state,
    stream::{
        channel::strategy::ChannelStrategy,
        operator::{Operator, OperatorContext},
    },
    util::SafelySendableFn,
};
use arcon_error::ArconResult;
use kompact::prelude::ComponentDefinition;
use std::sync::Arc;

pub mod collection;
//#[cfg(feature = "kafka")]
//pub mod kafka;
pub mod local_file;
#[cfg(feature = "socket")]
pub mod socket;

/// Message type Arcon sources must implement
#[derive(Debug, Clone)]
pub enum ArconSource {
    Epoch(Epoch),
    Start,
}

/// Common Context for all Source implementations
pub struct SourceContext<OP: Operator + 'static, B: state::Backend> {
    /// Timestamp extractor function
    ///
    /// If set to None, timestamps of ArconElement's will also be None.
    ts_extractor: Option<&'static dyn SafelySendableFn(&OP::IN) -> u64>,
    /// Current Watermark
    current_watermark: u64,
    /// Watermark interval
    ///
    /// Controls how often the source generates watermarks. For finite
    /// sources, `watermark_interval` may be an element counter. Whereas
    /// in an unbounded source type, it may be the timer timeout period.
    pub watermark_interval: u64,
    /// An Operator to enable fusion of computation within the source
    operator: OP,
    /// Strategy for outputting events
    channel_strategy: ChannelStrategy<OP::OUT>,
    /// Durable State Backend
    _backend: Arc<B>,
}

impl<OP, B> SourceContext<OP, B>
where
    OP: Operator + 'static,
    B: state::Backend,
{
    pub fn new(
        watermark_interval: u64,
        ts_extractor: Option<&'static dyn SafelySendableFn(&OP::IN) -> u64>,
        channel_strategy: ChannelStrategy<OP::OUT>,
        operator: OP,
        backend: Arc<B>,
    ) -> Self {
        SourceContext {
            ts_extractor,
            current_watermark: 0,
            watermark_interval,
            operator,
            channel_strategy,
            _backend: backend,
        }
    }

    /// Generates a Watermark event and sends it downstream
    #[inline]
    pub fn generate_watermark(&mut self, source: &impl ComponentDefinition) {
        let wm_event: ArconEvent<OP::OUT> = {
            if self.has_timestamp_extractor() {
                ArconEvent::Watermark(Watermark::new(self.current_watermark))
            } else {
                let system_time = crate::util::get_system_time();
                self.watermark_update(system_time);
                ArconEvent::Watermark(Watermark::new(self.current_watermark))
            }
        };

        self.channel_strategy.add(wm_event, source);
    }

    /// Inject epoch marker into the dataflow
    #[inline]
    pub fn inject_epoch(&mut self, epoch: Epoch, source: &impl ComponentDefinition) {
        self.channel_strategy.add(ArconEvent::Epoch(epoch), source);
    }

    /// Generates a Death event and sends it downstream
    #[inline]
    pub fn generate_death(&mut self, msg: String, source: &impl ComponentDefinition) {
        self.channel_strategy.add(ArconEvent::Death(msg), source);
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

    /// Calls a transformation function on the source data to generate outgoing events
    #[inline]
    pub fn process(
        &mut self,
        data: ArconElement<OP::IN>,
        source: &impl ComponentDefinition,
    ) -> ArconResult<()> {
        self.operator.handle_element(
            data,
            OperatorContext::<_, B, _>::new(source, &mut None, &mut self.channel_strategy),
        )
    }

    /// Build ArconElement
    ///
    /// Extracts timestamp if extractor is available
    #[inline]
    pub fn extract_element(&mut self, data: OP::IN) -> ArconElement<OP::IN> {
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
