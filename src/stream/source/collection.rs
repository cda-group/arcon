// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use super::SourceContext;
use crate::{
    prelude::state,
    stream::{operator::Operator, source::ArconSource},
};
use kompact::prelude::*;
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
pub struct CollectionSource<OP, B>
where
    OP: Operator + 'static,
    B: state::Backend,
{
    ctx: ComponentContext<Self>,
    loopback_send: RequiredPort<LoopbackPort>,
    loopback_receive: ProvidedPort<LoopbackPort>,
    pub source_ctx: RefCell<SourceContext<OP, B>>,
    collection: RefCell<Vec<OP::IN>>,
    counter: usize,
}

impl<OP, B> CollectionSource<OP, B>
where
    OP: Operator + 'static,
    B: state::Backend,
{
    pub fn new(collection: Vec<OP::IN>, source_ctx: SourceContext<OP, B>) -> Self {
        CollectionSource {
            ctx: ComponentContext::uninitialised(),
            loopback_send: RequiredPort::uninitialised(),
            loopback_receive: ProvidedPort::uninitialised(),
            source_ctx: RefCell::new(source_ctx),
            collection: RefCell::new(collection),
            counter: 0,
        }
    }
    fn process_collection(&mut self) {
        let mut collection = self.collection.borrow_mut();
        let drain_to = RESCHEDULE_EVERY.min(collection.len());
        let mut source_ctx = self.source_ctx.borrow_mut();
        for record in collection.drain(..drain_to) {
            let elem = source_ctx.extract_element(record);
            if let Err(err) = source_ctx.process(elem, self) {
                error!(self.ctx.log(), "Error while processing record {:?}", err);
            }

            self.counter += 1;
            if (self.counter as u64) == source_ctx.watermark_interval {
                source_ctx.generate_watermark(self);
                self.counter = 0;
            }
        }
        if !collection.is_empty() {
            self.loopback_send.trigger(ContinueSending);
        } else {
            source_ctx.generate_watermark(self);
        }
    }
}

impl<OP, B> ComponentLifecycle for CollectionSource<OP, B>
where
    OP: Operator + 'static,
    B: state::Backend,
{
    fn on_start(&mut self) -> Handled {
        let shared = self.loopback_receive.share();
        self.loopback_send.connect(shared);
        self.loopback_send.trigger(ContinueSending);
        Handled::Ok
    }
}

impl<OP, B> Provide<LoopbackPort> for CollectionSource<OP, B>
where
    OP: Operator + 'static,
    B: state::Backend,
{
    fn handle(&mut self, _event: ContinueSending) -> Handled {
        self.process_collection();
        Handled::Ok
    }
}

impl<OP, B> Require<LoopbackPort> for CollectionSource<OP, B>
where
    OP: Operator + 'static,
    B: state::Backend,
{
    fn handle(&mut self, _event: Never) -> Handled {
        unreachable!("Never type has no instance");
    }
}

impl<OP, B> Actor for CollectionSource<OP, B>
where
    OP: Operator + 'static,
    B: state::Backend,
{
    type Message = ArconSource;
    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        match msg {
            ArconSource::Epoch(epoch) => {
                self.source_ctx.borrow_mut().inject_epoch(epoch, self);
            }
        }

        Handled::Ok
    }
    fn receive_network(&mut self, _: NetMessage) -> Handled {
        unreachable!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data::ArconMessage,
        pipeline::Pipeline,
        prelude::{Channel, ChannelStrategy, DebugNode, Filter, Forward},
    };
    use std::sync::Arc;

    #[test]
    fn collection_source_test() {
        let mut pipeline = Pipeline::new();
        let pool_info = pipeline.get_pool_info();
        let system = pipeline.system();

        let sink = system.create(move || DebugNode::<u64>::new());
        system
            .start_notify(&sink)
            .wait_timeout(std::time::Duration::from_millis(100))
            .expect("started");

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
        let backend = Arc::new(crate::util::temp_backend());

        let source_context = SourceContext::new(
            watermark_interval,
            None, // no timestamp extractor
            channel_strategy,
            Filter::new(&filter_fn),
            backend,
        );

        // Generate collection
        let collection: Vec<u64> = (0..collection_elements).collect();

        // Set up CollectionSource component
        let collection_source = CollectionSource::new(collection, source_context);
        let source_comp = system.create(move || collection_source);
        system
            .start_notify(&source_comp)
            .wait_timeout(std::time::Duration::from_millis(100))
            .expect("started");

        // Wait a bit in order for all results to come in...
        std::thread::sleep(std::time::Duration::from_secs(1));

        sink.on_definition(|cd| {
            let data_len = cd.data.len();
            let watermark_len = cd.watermarks.len();

            assert_eq!(
                watermark_len as u64,
                (collection_elements / watermark_interval) + 1 // One more is generated after the loop
            );
            assert_eq!(data_len, 1000);
        });

        pipeline.shutdown();
    }
}
