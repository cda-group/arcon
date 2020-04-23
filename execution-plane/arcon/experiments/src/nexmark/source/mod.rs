// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::nexmark::{config::NEXMarkConfig, NEXMarkEvent};
use arcon::prelude::*;
use rand::{rngs::SmallRng, FromEntropy};

const RESCHEDULE_EVERY: usize = 500000;

#[derive(Debug, Clone, Copy)]
struct ContinueSending;
struct LoopbackPort;
impl Port for LoopbackPort {
    type Indication = Never;
    type Request = ContinueSending;
}

#[derive(ComponentDefinition)]
#[allow(dead_code)]
pub struct NEXMarkSource<OP>
where
    OP: Operator<IN = NEXMarkEvent> + 'static,
{
    ctx: ComponentContext<Self>,
    loopback_send: RequiredPort<LoopbackPort, Self>,
    loopback_receive: ProvidedPort<LoopbackPort, Self>,
    source_ctx: SourceContext<OP>,
    nexmark_config: NEXMarkConfig,
    timer: ::std::time::Instant,
    events_so_far: u32,
    watermark_counter: u64,
    duration_ns: u64,
}

impl<OP> NEXMarkSource<OP>
where
    OP: Operator<IN = NEXMarkEvent> + 'static,
{
    pub fn new(mut nexmark_config: NEXMarkConfig, source_ctx: SourceContext<OP>) -> Self {
        let timer = ::std::time::Instant::now();
        // Establish a start of the computation.
        let elapsed = timer.elapsed();
        let elapsed_ns =
            (elapsed.as_secs() * 1_000_000_000 + (elapsed.subsec_nanos() as u64)) as usize;
        nexmark_config.base_time_ns = elapsed_ns as u32;
        let duration_ns: u64 = nexmark_config.stream_timeout * 1_000_000_000;
        NEXMarkSource {
            ctx: ComponentContext::new(),
            loopback_send: RequiredPort::new(),
            loopback_receive: ProvidedPort::new(),
            source_ctx,
            nexmark_config,
            timer,
            events_so_far: 0,
            watermark_counter: 0,
            duration_ns,
        }
    }

    pub fn process(&mut self) {
        let mut rng = SmallRng::from_entropy();
        let mut iter_counter = 0;

        while iter_counter < RESCHEDULE_EVERY && self.events_so_far < self.nexmark_config.num_events
        {
            let next_event = crate::nexmark::NEXMarkEvent::create(
                self.events_so_far,
                &mut rng,
                &mut self.nexmark_config,
            );

            let elem = self.source_ctx.extract_element(next_event);
            debug!(self.ctx().log(), "Created elem {:?}", elem);
            self.source_ctx.process(elem);
            self.events_so_far += 1;

            self.watermark_counter += 1;
            if self.watermark_counter == self.source_ctx.watermark_interval {
                self.source_ctx.generate_watermark();
                self.watermark_counter = 0;
            }

            iter_counter += 1;
        }

        if iter_counter == RESCHEDULE_EVERY {
            self.loopback_send.trigger(ContinueSending);
        } else {
            // We are finished ...
            // Set watermark to max value to ensure everything triggers...
            self.source_ctx.watermark_update(u64::max_value());
            self.source_ctx.generate_watermark();
            // send death msg
            self.source_ctx
                .generate_death(String::from("nexmark_finished"));
            info!(
                self.ctx().log(),
                "Finished generating {} events", self.nexmark_config.num_events
            );
        }
    }
}

impl<OP> Provide<ControlPort> for NEXMarkSource<OP>
where
    OP: Operator<IN = NEXMarkEvent> + 'static,
{
    fn handle(&mut self, event: ControlEvent) {
        if let ControlEvent::Start = event {
            let shared = self.loopback_receive.share();
            self.loopback_send.connect(shared);
            self.loopback_send.trigger(ContinueSending);
        }
    }
}

impl<OP> Provide<LoopbackPort> for NEXMarkSource<OP>
where
    OP: Operator<IN = NEXMarkEvent> + 'static,
{
    fn handle(&mut self, _event: ContinueSending) {
        self.process();
    }
}

impl<OP> Require<LoopbackPort> for NEXMarkSource<OP>
where
    OP: Operator<IN = NEXMarkEvent> + 'static,
{
    fn handle(&mut self, _event: Never) {
        unreachable!("Never type has no instance");
    }
}

impl<OP> Actor for NEXMarkSource<OP>
where
    OP: Operator<IN = NEXMarkEvent> + 'static,
{
    type Message = ();
    fn receive_local(&mut self, _msg: Self::Message) {}
    fn receive_network(&mut self, _msg: NetMessage) {}
}
