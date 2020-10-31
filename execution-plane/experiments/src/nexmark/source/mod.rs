// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::nexmark::{config::NEXMarkConfig, NEXMarkEvent};
use arcon::{prelude::*, timer::TimerBackend};
use rand::{rngs::SmallRng, FromEntropy};
use std::cell::RefCell;

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
pub struct NEXMarkSource<OP, B, T>
where
    OP: Operator<B, IN = NEXMarkEvent, TimerState = ArconNever> + 'static,
    B: state::Backend,
    T: TimerBackend<ArconNever>,
{
    ctx: ComponentContext<Self>,
    loopback_send: RequiredPort<LoopbackPort>,
    loopback_receive: ProvidedPort<LoopbackPort>,
    source_ctx: RefCell<SourceContext<OP, B, T>>,
    nexmark_config: NEXMarkConfig,
    timer: ::std::time::Instant,
    events_so_far: u32,
    watermark_counter: u64,
    duration_ns: u64,
}

impl<OP, B, T> NEXMarkSource<OP, B, T>
where
    OP: Operator<B, IN = NEXMarkEvent, TimerState = ArconNever> + 'static,
    B: state::Backend,
    T: TimerBackend<ArconNever>,
{
    pub fn new(mut nexmark_config: NEXMarkConfig, source_ctx: SourceContext<OP, B, T>) -> Self {
        let timer = ::std::time::Instant::now();
        // Establish a start of the computation.
        let elapsed = timer.elapsed();
        let elapsed_ns =
            (elapsed.as_secs() * 1_000_000_000 + (elapsed.subsec_nanos() as u64)) as usize;
        nexmark_config.base_time_ns = elapsed_ns as u32;
        let duration_ns: u64 = nexmark_config.stream_timeout * 1_000_000_000;
        NEXMarkSource {
            ctx: ComponentContext::uninitialised(),
            loopback_send: RequiredPort::uninitialised(),
            loopback_receive: ProvidedPort::uninitialised(),
            source_ctx: RefCell::new(source_ctx),
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
        let mut source_ctx = self.source_ctx.borrow_mut();

        while iter_counter < RESCHEDULE_EVERY && self.events_so_far < self.nexmark_config.num_events
        {
            let next_event = crate::nexmark::NEXMarkEvent::create(
                self.events_so_far,
                &mut rng,
                &mut self.nexmark_config,
            );

            let elem = source_ctx.extract_element(next_event);
            debug!(self.ctx().log(), "Created elem {:?}", elem);
            source_ctx.process(elem, self);
            self.events_so_far += 1;

            self.watermark_counter += 1;
            if self.watermark_counter == source_ctx.watermark_interval {
                source_ctx.generate_watermark(self);
                self.watermark_counter = 0;
            }

            iter_counter += 1;
        }

        if iter_counter == RESCHEDULE_EVERY {
            self.loopback_send.trigger(ContinueSending);
        } else {
            // We are finished ...
            // Set watermark to max value to ensure everything triggers...
            source_ctx.watermark_update(u64::max_value());
            source_ctx.generate_watermark(self);
            // send death msg
            source_ctx.generate_death(String::from("nexmark_finished"), self);
            info!(
                self.ctx().log(),
                "Finished generating {} events", self.nexmark_config.num_events
            );
        }
    }
}

impl<OP, B, T> ComponentLifecycle for NEXMarkSource<OP, B, T>
where
    OP: Operator<B, IN = NEXMarkEvent, TimerState = ArconNever> + 'static,
    B: state::Backend,
    T: TimerBackend<ArconNever>,
{
    fn on_start(&mut self) -> Handled {
        let shared = self.loopback_receive.share();
        self.loopback_send.connect(shared);
        self.loopback_send.trigger(ContinueSending);
        Handled::Ok
    }
}

impl<OP, B, T> Provide<LoopbackPort> for NEXMarkSource<OP, B, T>
where
    OP: Operator<B, IN = NEXMarkEvent, TimerState = ArconNever> + 'static,
    B: state::Backend,
    T: TimerBackend<ArconNever>,
{
    fn handle(&mut self, _event: ContinueSending) -> Handled {
        self.process();
        Handled::Ok
    }
}

impl<OP, B, T> Require<LoopbackPort> for NEXMarkSource<OP, B, T>
where
    OP: Operator<B, IN = NEXMarkEvent, TimerState = ArconNever> + 'static,
    B: state::Backend,
    T: TimerBackend<ArconNever>,
{
    fn handle(&mut self, _event: Never) -> Handled {
        unreachable!("Never type has no instance");
    }
}

impl<OP, B, T> Actor for NEXMarkSource<OP, B, T>
where
    OP: Operator<B, IN = NEXMarkEvent, TimerState = ArconNever> + 'static,
    B: state::Backend,
    T: TimerBackend<ArconNever>,
{
    type Message = ();
    fn receive_local(&mut self, _msg: Self::Message) -> Handled {
        unreachable!();
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        unreachable!();
    }
}
