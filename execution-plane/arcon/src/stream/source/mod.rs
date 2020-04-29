// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconElement, ArconEvent, Watermark},
    state_backend::StateBackend,
    stream::{
        channel::strategy::ChannelStrategy,
        operator::{Operator, OperatorContext},
    },
    timer::TimerBackend,
    util::SafelySendableFn,
};

pub mod collection;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod local_file;
#[cfg(feature = "socket")]
pub mod socket;

/// Common Context for all Source implementations
pub struct SourceContext<OP: Operator> {
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
    /// State backend that a source can keep persistent data in
    pub state_backend: Box<dyn StateBackend>,
    /// Timer Backend to keep track of event timers
    timer_backend: Box<dyn TimerBackend<OP::TimerState>>,
}

impl<OP: Operator> SourceContext<OP> {
    pub fn new<F>(
        watermark_interval: u64,
        ts_extractor: Option<&'static dyn SafelySendableFn(&OP::IN) -> u64>,
        channel_strategy: ChannelStrategy<OP::OUT>,
        operator: OP,
        mut state_backend: Box<dyn StateBackend>,
        timer_backend_fn: F,
    ) -> Self
    where
        F: Fn(&mut dyn StateBackend) -> Box<dyn TimerBackend<OP::TimerState>> + Sized + 'static,
    {
        let timer_backend = timer_backend_fn(state_backend.as_mut());
        SourceContext {
            ts_extractor,
            current_watermark: 0,
            watermark_interval,
            operator,
            channel_strategy,
            state_backend,
            timer_backend,
        }
    }

    /// Generates a Watermark event and sends it downstream
    #[inline]
    pub fn generate_watermark(&mut self) {
        let wm_event: ArconEvent<OP::OUT> = {
            if self.has_timestamp_extractor() {
                ArconEvent::Watermark(Watermark::new(self.current_watermark))
            } else {
                let system_time = crate::util::get_system_time();
                self.watermark_update(system_time);
                ArconEvent::Watermark(Watermark::new(self.current_watermark))
            }
        };

        self.channel_strategy.add(wm_event);
    }

    /// Generates a Death event and sends it downstream
    #[inline]
    pub fn generate_death(&mut self, msg: String) {
        self.channel_strategy.add(ArconEvent::Death(msg));
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
    pub fn process(&mut self, data: ArconElement<OP::IN>) {
        self.operator.handle_element(
            data,
            OperatorContext::new(
                &mut self.channel_strategy,
                self.state_backend.as_mut(),
                self.timer_backend.as_mut(),
            ),
        );
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
