// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use super::SourceContext;
use crate::data::ArconType;
use kompact::prelude::*;

#[derive(ComponentDefinition)]
pub struct CollectionSource<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    ctx: ComponentContext<Self>,
    source_ctx: SourceContext<IN, OUT>,
    collection: Option<Vec<IN>>,
}

impl<IN, OUT> CollectionSource<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    pub fn new(collection: Vec<IN>, source_ctx: SourceContext<IN, OUT>) -> Self {
        CollectionSource {
            ctx: ComponentContext::new(),
            source_ctx,
            collection: Some(collection),
        }
    }
    pub fn process_collection(&mut self) {
        let mut counter: u64 = 0;
        let system = &self.ctx().system();
        for record in self.collection.take().unwrap() {
            let elem = self.source_ctx.extract_element(record);
            self.source_ctx.process(elem, system);

            counter += 1;

            if counter == self.source_ctx.watermark_interval {
                self.source_ctx.generate_watermark(system);
                counter = 0;
            }
        }
    }
}

impl<IN, OUT> Provide<ControlPort> for CollectionSource<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    fn handle(&mut self, event: ControlEvent) -> () {
        if let ControlEvent::Start = event {
            self.process_collection();
        }
    }
}

impl<IN, OUT> Actor for CollectionSource<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    type Message = ();
    fn receive_local(&mut self, _msg: Self::Message) {}
    fn receive_network(&mut self, _msg: NetMessage) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::{Channel, ChannelStrategy, DebugNode, Filter, Forward, NodeID};
    use kompact::default_components::DeadletterBox;
    use kompact::prelude::KompactSystem;
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

        let operator = Box::new(Filter::<u64>::new(&filter_fn));

        // Set up SourceContext
        let buffer_limit = 200;
        let watermark_interval = 50;
        let collection_elements = 2000;

        let source_context = SourceContext::new(
            buffer_limit,
            watermark_interval,
            None, // no timestamp extractor
            channel_strategy,
            operator,
        );

        // Generate collection
        let collection: Vec<u64> = (0..collection_elements).collect();

        // Set up CollectionSource component
        let collection_source: CollectionSource<u64, u64> =
            CollectionSource::new(collection, source_context);
        let source = system.create(move || collection_source);
        system.start(&source);

        // Wait a bit in order for all results to come in...
        std::thread::sleep(std::time::Duration::from_secs(1));

        let sink_inspect = sink.definition().lock().unwrap();
        let data_len = sink_inspect.data.len();
        let watermark_len = sink_inspect.watermarks.len();

        assert_eq!(
            watermark_len as u64,
            collection_elements / watermark_interval
        );
        assert_eq!(data_len, 1000);
    }
}
