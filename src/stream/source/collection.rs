// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{Source, SourceContext};
use crate::{data::ArconType, dataflow::conf::SourceConf, stream::time::ArconTime};
use kompact::prelude::*;
use std::cell::RefCell;

const RESCHEDULE_EVERY: usize = 10000;

pub struct CollectionSource<A>
where
    A: ArconType,
{
    data: RefCell<Vec<A>>,
    conf: SourceConf<A>,
}

impl<A> CollectionSource<A>
where
    A: ArconType,
{
    pub fn new(data: Vec<A>, conf: SourceConf<A>) -> Self {
        Self {
            data: RefCell::new(data),
            conf,
        }
    }
}

impl<A> Source for CollectionSource<A>
where
    A: ArconType,
{
    type Data = A;

    fn process_batch(&self, mut ctx: SourceContext<Self, impl ComponentDefinition>) {
        let drain_to = RESCHEDULE_EVERY.min(self.data.borrow().len());
        for record in self.data.borrow_mut().drain(..drain_to) {
            match &self.conf.time {
                ArconTime::Event => match &self.conf.extractor {
                    Some(extractor) => {
                        let timestamp = extractor(&record);
                        ctx.output_with_timestamp(record, timestamp);
                    }
                    None => panic!("Cannot use ArconTime::Event without an timestamp extractor"),
                },
                ArconTime::Process => ctx.output(record),
            }
        }
        if self.data.borrow().is_empty() {
            ctx.signal_end();
        }
    }
}

/*
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
        let mut pipeline = Pipeline::default();
        let pool_info = pipeline.get_pool_info();
        let system = pipeline.system();

        let sink = system.create(DebugNode::<u64>::new);
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

        source_comp.actor_ref().tell(ArconSource::Start);

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
*/
