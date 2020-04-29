// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use super::SourceContext;
use crate::stream::operator::Operator;
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
pub struct CollectionSource<OP>
where
    OP: Operator + 'static,
{
    ctx: ComponentContext<Self>,
    loopback_send: RequiredPort<LoopbackPort, Self>,
    loopback_receive: ProvidedPort<LoopbackPort, Self>,
    pub source_ctx: SourceContext<OP>,
    collection: Vec<OP::IN>,
    counter: usize,
}

impl<OP> CollectionSource<OP>
where
    OP: Operator + 'static,
{
    pub fn new(collection: Vec<OP::IN>, source_ctx: SourceContext<OP>) -> Self {
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

impl<OP> Provide<ControlPort> for CollectionSource<OP>
where
    OP: Operator + 'static,
{
    fn handle(&mut self, event: ControlEvent) {
        if let ControlEvent::Start = event {
            let shared = self.loopback_receive.share();
            self.loopback_send.connect(shared);
            self.loopback_send.trigger(ContinueSending);
        }
    }
}

impl<OP> Provide<LoopbackPort> for CollectionSource<OP>
where
    OP: Operator + 'static,
{
    fn handle(&mut self, _event: ContinueSending) {
        self.process_collection();
    }
}

impl<OP> Require<LoopbackPort> for CollectionSource<OP>
where
    OP: Operator + 'static,
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
        data::ArconType,
        prelude::{Channel, ChannelStrategy, DebugNode, Filter, Forward, NodeID},
        state_backend::{in_memory::InMemory, StateBackend},
        timer,
    };
    use kompact::{default_components::DeadletterBox, prelude::KompactSystem};
    use std::sync::Arc;

    // Perhaps move this to some common place?
    fn test_setup<A: ArconType>() -> (KompactSystem, Arc<Component<DebugNode<A>>>) {
        // Kompact set-up
        let mut cfg = KompactConfig::new();
        cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
        let system = cfg.build().expect("KompactSystem");

        let (sink, _) = system.create_and_register(move || {
            let s: DebugNode<A> = DebugNode::new();
            s
        });

        system.start(&sink);

        return (system, sink);
    }

    #[test]
    fn collection_source_test() {
        let (system, sink) = test_setup::<u64>();
        // Configure channel strategy for sink
        let actor_ref = sink.actor_ref().hold().expect("fail");
        let channel = Channel::Local(actor_ref);
        let channel_strategy = ChannelStrategy::Forward(Forward::new(channel, NodeID::new(1)));

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
            Filter::<u64>::new(&filter_fn),
            Box::new(InMemory::new("test".as_ref()).unwrap()),
            timer::none,
        );

        // Generate collection
        let collection: Vec<u64> = (0..collection_elements).collect();

        // Set up CollectionSource component
        let collection_source = CollectionSource::new(collection, source_context);
        let source = system.create(move || collection_source);
        system.start(&source);

        // Wait a bit in order for all results to come in...
        std::thread::sleep(std::time::Duration::from_secs(1));

        let sink_inspect = sink.definition().lock().unwrap();
        let data_len = sink_inspect.data.len();
        let watermark_len = sink_inspect.watermarks.len();

        assert_eq!(
            watermark_len as u64,
            (collection_elements / watermark_interval) + 1 // One more is generated after the loop
        );
        assert_eq!(data_len, 1000);
    }
}
