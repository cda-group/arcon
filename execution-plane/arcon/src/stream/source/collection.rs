// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use super::SourceContext;
use crate::{prelude::state, stream::operator::Operator, timer::TimerBackend};
use kompact::prelude::*;

const RESCHEDULE_EVERY: usize = 500000;

#[derive(Debug, Clone, Copy)]
struct ContinueSending;
struct LoopbackPort;
impl Port for LoopbackPort {
    type Indication = Never;
    type Request = ContinueSending;
}

#[derive(ComponentDefinition, Actor)]
pub struct CollectionSource<OP, B, T>
where
    OP: Operator<B> + 'static,
    B: state::Backend,
    T: TimerBackend<OP::TimerState>,
{
    ctx: ComponentContext<Self>,
    loopback_send: RequiredPort<LoopbackPort, Self>,
    loopback_receive: ProvidedPort<LoopbackPort, Self>,
    pub source_ctx: SourceContext<OP, B, T>,
    collection: Vec<OP::IN>,
    counter: usize,
}

impl<OP, B, T> CollectionSource<OP, B, T>
where
    OP: Operator<B> + 'static,
    B: state::Backend,
    T: TimerBackend<OP::TimerState>,
{
    pub fn new(collection: Vec<OP::IN>, source_ctx: SourceContext<OP, B, T>) -> Self {
        CollectionSource {
            ctx: ComponentContext::new(),
            loopback_send: RequiredPort::new(),
            loopback_receive: ProvidedPort::new(),
            source_ctx,
            collection,
            counter: 0,
        }
    }
    fn process_collection(&mut self) {
        let drain_to = RESCHEDULE_EVERY.min(self.collection.len());
        for record in self.collection.drain(..drain_to) {
            let elem = self.source_ctx.extract_element(record);
            self.source_ctx.process(elem);

            self.counter += 1;
            if (self.counter as u64) == self.source_ctx.watermark_interval {
                self.source_ctx.generate_watermark();
                self.counter = 0;
            }
        }
        if !self.collection.is_empty() {
            self.loopback_send.trigger(ContinueSending);
        } else {
            self.source_ctx.generate_watermark();
        }
    }
}

impl<OP, B, T> Provide<ControlPort> for CollectionSource<OP, B, T>
where
    OP: Operator<B> + 'static,
    B: state::Backend,
    T: TimerBackend<OP::TimerState>,
{
    fn handle(&mut self, event: ControlEvent) {
        if let ControlEvent::Start = event {
            let shared = self.loopback_receive.share();
            self.loopback_send.connect(shared);
            self.loopback_send.trigger(ContinueSending);
        }
    }
}

impl<OP, B, T> Provide<LoopbackPort> for CollectionSource<OP, B, T>
where
    OP: Operator<B> + 'static,
    B: state::Backend,
    T: TimerBackend<OP::TimerState>,
{
    fn handle(&mut self, _event: ContinueSending) {
        self.process_collection();
    }
}

impl<OP, B, T> Require<LoopbackPort> for CollectionSource<OP, B, T>
where
    OP: Operator<B> + 'static,
    B: state::Backend,
    T: TimerBackend<OP::TimerState>,
{
    fn handle(&mut self, _event: Never) {
        unreachable!("Never type has no instance");
    }
}

//ignore_indications!(LoopbackPort, CollectionSource);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data::ArconMessage,
        pipeline::ArconPipeline,
        prelude::{Channel, ChannelStrategy, DebugNode, Filter, Forward},
        state::{Backend, InMemory},
        timer,
    };

    #[test]
    fn collection_source_test() {
        let mut pipeline = ArconPipeline::new();
        let pool_info = pipeline.get_pool_info();
        let system = pipeline.system();

        let sink = system.create(move || DebugNode::<u64>::new());
        system.start(&sink);

        // Configure channel strategy for sink
        let actor_ref: ActorRefStrong<ArconMessage<u64>> =
            sink.actor_ref().hold().expect("failed to fetch");
        let channel_strategy =
            ChannelStrategy::Forward(Forward::new(Channel::Local(actor_ref), 1.into(), pool_info));

        // Our operator function
        fn filter_fn(x: &u64) -> bool {
            *x < 1000
        }

        // Set up SourceContext
        let watermark_interval = 50;
        let collection_elements = 2000;

        let source_context = SourceContext::new(
            watermark_interval,
            None, // no timestamp extractor
            channel_strategy,
            Filter::new(&filter_fn),
            InMemory::create("test".as_ref()).unwrap(),
            timer::none(),
        );

        // Generate collection
        let collection: Vec<u64> = (0..collection_elements).collect();

        // Set up CollectionSource component
        let collection_source = CollectionSource::new(collection, source_context);
        let source = system.create(move || collection_source);
        system.start(&source);

        // Wait a bit in order for all results to come in...
        std::thread::sleep(std::time::Duration::from_secs(1));

        {
            let sink_inspect = sink.definition().lock().unwrap();
            let data_len = sink_inspect.data.len();
            let watermark_len = sink_inspect.watermarks.len();

            assert_eq!(
                watermark_len as u64,
                (collection_elements / watermark_interval) + 1 // One more is generated after the loop
            );
            assert_eq!(data_len, 1000);
        }

        pipeline.shutdown();
    }
}
