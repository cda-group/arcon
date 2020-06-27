// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    buffer::event::{BufferPool, BufferWriter, PoolInfo},
    data::{ArconEvent, ArconType},
    prelude::*,
    stream::channel::strategy::send,
};
use kompact::prelude::ComponentDefinition;
use std::collections::HashMap;

/// A hash based partitioner
pub struct KeyBy<A>
where
    A: ArconType,
{
    /// A buffer pool of EventBuffer's
    buffer_pool: BufferPool<ArconEventWrapper<A>>,
    /// Used to hash % modulo
    parallelism: u32,
    /// An identifier that is embedded with outgoing messages
    sender_id: NodeID,
    /// A map with hashed indexes and their respective Channel/Buffer
    buffer_map: HashMap<usize, (Channel<A>, BufferWriter<ArconEventWrapper<A>>)>,
    /// Struct holding information regarding the BufferPool
    _pool_info: PoolInfo,
}

impl<A> KeyBy<A>
where
    A: ArconType,
{
    /// Creates a KeyBy strategy with Rust's default hasher
    pub fn new(channels: Vec<Channel<A>>, sender_id: NodeID, pool_info: PoolInfo) -> KeyBy<A> {
        let channels_len: u32 = channels.len() as u32;
        assert!(
            channels.len() < pool_info.capacity,
            "Strategy must be initialised with a pool capacity larger than amount of channels"
        );
        let mut buffer_pool: BufferPool<ArconEventWrapper<A>> = BufferPool::new(
            pool_info.capacity,
            pool_info.buffer_size,
            pool_info.allocator.clone(),
        )
        .expect("failed to initialise BufferPool");

        let mut buffer_map = HashMap::new();
        for (i, channel) in channels.into_iter().enumerate() {
            let writer = buffer_pool
                .try_get()
                .expect("failed to fetch initial buffer");
            buffer_map.insert(i, (channel, writer));
        }

        KeyBy {
            buffer_pool,
            parallelism: channels_len,
            sender_id,
            buffer_map,
            _pool_info: pool_info,
        }
    }

    #[inline]
    pub fn add<CD>(&mut self, event: ArconEvent<A>, source: &CD)
    where
        CD: ComponentDefinition + Sized + 'static,
    {
        match &event {
            ArconEvent::Element(element) => {
                let hash = element.data.get_key() as u32;
                let index = (hash % self.parallelism) as usize;
                if let Some((chan, buffer)) = self.buffer_map.get_mut(&index) {
                    if let Some(e) = buffer.push(event.into()) {
                        // buffer is full, flush.
                        // NOTE: we are just flushing a single buffer
                        let msg = ArconMessage {
                            events: buffer.reader(),
                            sender: self.sender_id,
                        };
                        send(chan, msg, source);
                        // set new writer
                        *buffer = self.buffer_pool.get();
                        let _ = buffer.push(e.into());
                    }
                } else {
                    panic!("Bad KeyBy setup");
                }
            }
            _ => {
                // Push watermark/epoch into all outgoing buffers
                for (_, (_, buffer)) in self.buffer_map.iter_mut() {
                    buffer.push(event.clone().into());
                }
                self.flush(source);
            }
        }
    }

    #[inline]
    pub fn flush<CD>(&mut self, source: &CD)
    where
        CD: ComponentDefinition + Sized + 'static,
    {
        for (_, (ref channel, buffer)) in self.buffer_map.iter_mut() {
            let msg = ArconMessage {
                events: buffer.reader(),
                sender: self.sender_id,
            };
            send(channel, msg, source);
            // get a new writer
            *buffer = self.buffer_pool.get();
        }
    }

    #[inline]
    pub fn num_channels(&self) -> usize {
        self.buffer_map.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{data::ArconEvent, pipeline::ArconPipeline, stream::channel::strategy::tests::*};
    use kompact::prelude::*;
    use rand::Rng;
    use std::sync::Arc;

    #[test]
    fn keyby_test() {
        let mut pipeline = ArconPipeline::new();
        let pool_info = pipeline.get_pool_info();
        let system = pipeline.system();

        let parallelism: u32 = 8;
        let total_msgs = 1000;

        let mut channels: Vec<Channel<Input>> = Vec::new();
        let mut comps: Vec<Arc<crate::prelude::Component<DebugNode<Input>>>> = Vec::new();

        for _i in 0..parallelism {
            let comp = system.create(move || DebugNode::<Input>::new());
            system.start(&comp);
            let actor_ref: ActorRefStrong<ArconMessage<Input>> =
                comp.actor_ref().hold().expect("failed to fetch");
            channels.push(Channel::Local(actor_ref));
            comps.push(comp);
        }

        let mut channel_strategy =
            ChannelStrategy::KeyBy(KeyBy::new(channels, NodeID::new(1), pool_info));

        let mut rng = rand::thread_rng();

        let mut inputs: Vec<ArconEvent<Input>> = Vec::new();
        for _i in 0..total_msgs {
            let input = Input {
                id: rng.gen_range(0, 100),
            };
            let elem = ArconElement::new(input);
            inputs.push(ArconEvent::Element(elem));
        }

        // take one comp as channel source
        // just for testing...
        let comp = &comps[0];
        comp.on_definition(|cd| {
            for input in inputs {
                let _ = channel_strategy.add(input, cd);
            }
            let _ = channel_strategy.flush(cd);
        });

        std::thread::sleep(std::time::Duration::from_secs(1));

        // Each of the 8 components should at least get some hits
        for comp in comps {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert!(comp_inspect.data.len() > 0);
        }
        pipeline.shutdown();
    }
}
