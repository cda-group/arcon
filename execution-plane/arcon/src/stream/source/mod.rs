// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconElement, ArconEvent, Watermark},
    prelude::state,
    stream::{
        channel::strategy::ChannelStrategy,
        operator::{Operator, OperatorContext},
    },
    timer::TimerBackend,
    util::SafelySendableFn,
};
use kompact::prelude::ComponentDefinition;

pub mod collection;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod local_file;
#[cfg(feature = "socket")]
pub mod socket;

/// Common Context for all Source implementations
pub struct SourceContext<OP: Operator<B>, B: state::Backend, T: TimerBackend<OP::TimerState>> {
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
    pub state_backend: state::BackendContainer<B>,
    /// Timer Backend to keep track of event timers
    timer_backend: T,
}

impl<OP, B, T> SourceContext<OP, B, T>
where
    OP: Operator<B>,
    B: state::Backend,
    T: TimerBackend<OP::TimerState>,
{
    pub fn new(
        watermark_interval: u64,
        ts_extractor: Option<&'static dyn SafelySendableFn(&OP::IN) -> u64>,
        channel_strategy: ChannelStrategy<OP::OUT>,
        mut operator: OP,
        state_backend: state::BackendContainer<B>,
        mut timer_backend: T,
    ) -> Self {
        let mut sb_session = state_backend.session();

        // register all the states that will ever be used by this Node
        {
            // SAFETY: we specifically want this to be the only place that is supposed to call this
            let mut registration_token = unsafe { state::RegistrationToken::new(&mut sb_session) };

            operator.register_states(&mut registration_token);
            timer_backend.register_states(&mut registration_token);
        }

        operator.init(&mut sb_session);
        timer_backend.init(&mut sb_session);

        drop(sb_session);

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
    pub fn generate_watermark<CD>(&mut self, source: &CD)
    where
        CD: ComponentDefinition + Sized + 'static,
    {
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

    /// Generates a Death event and sends it downstream
    #[inline]
    pub fn generate_death<CD>(&mut self, msg: String, source: &CD)
    where
        CD: ComponentDefinition + Sized + 'static,
    {
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

    /// Calls a transformation function on the source data to generate outgoing ArconEvent<OUT>
    #[inline]
    pub fn process<CD>(&mut self, data: ArconElement<OP::IN>, source: &CD)
    where
        CD: ComponentDefinition + Sized + 'static,
    {
        let mut session = self.state_backend.session();
        let events = self.operator.handle_element(
            data,
            OperatorContext::new(&mut session, &mut self.timer_backend),
        );
        for event in events {
            self.channel_strategy.add(event, source);
        }
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
