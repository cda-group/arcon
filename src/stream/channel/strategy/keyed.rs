// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    buffer::event::{BufferPool, BufferWriter, PoolInfo},
    data::{ArconEvent, ArconEventWrapper, ArconMessage, ArconType, NodeID},
    stream::channel::{strategy::send, Channel},
};
use fxhash::FxHashMap;
use kompact::prelude::{ComponentDefinition, SerError};

/// A Channel Strategy for Keyed Data Streams
///
/// Data is split onto a contiguous key space containing N key ranges.
pub struct Keyed<A>
where
    A: ArconType,
{
    /// A buffer pool of EventBuffer's
    buffer_pool: BufferPool<ArconEventWrapper<A>>,
    /// The highest possible key value
    ///
    /// This should not be set too low or ridiculously high
    max_key: u64,
    /// Number of ranges on the contiguous key space
    key_ranges: u64,
    /// An identifier that is embedded with outgoing messages
    sender_id: NodeID,
    /// A map with a key range id and its respective Channel/Buffer
    buffer_map: FxHashMap<usize, (Channel<A>, BufferWriter<ArconEventWrapper<A>>)>,
    /// Struct holding information regarding the BufferPool
    _pool_info: PoolInfo,
}

impl<A> Keyed<A>
where
    A: ArconType,
{
    /// Creates a Keyed strategy
    pub fn new(
        max_key: u64,
        channels: Vec<Channel<A>>,
        sender_id: NodeID,
        pool_info: PoolInfo,
    ) -> Keyed<A> {
        let channels_len: u64 = channels.len() as u64;
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

        let mut buffer_map = FxHashMap::default();
        for (i, channel) in channels.into_iter().enumerate() {
            let writer = buffer_pool
                .try_get()
                .expect("failed to fetch initial buffer");
            buffer_map.insert(i, (channel, writer));
        }

        Keyed {
            buffer_pool,
            key_ranges: channels_len,
            max_key,
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
                // Get key placement
                let key = element.data.get_key() % self.max_key;
                // Calculate which key range index is responsible for this key
                let index = (key * self.key_ranges / self.max_key) as usize;

                if let Some((chan, buffer)) = self.buffer_map.get_mut(&index) {
                    if let Some(e) = buffer.push(event.into()) {
                        // buffer is full
                        Self::flush_buffer(
                            self.sender_id,
                            &mut self.buffer_pool,
                            chan,
                            buffer,
                            source,
                        );
                        // This push should now not fail
                        let _ = buffer.push(e);
                    }
                } else {
                    panic!("Bad Keyed setup");
                }
            }
            _ => {
                // Push watermark/epoch into all outgoing buffers
                for (_, (chan, buffer)) in self.buffer_map.iter_mut() {
                    if let Some(e) = buffer.push(event.clone().into()) {
                        // buffer is full...
                        Self::flush_buffer(
                            self.sender_id,
                            &mut self.buffer_pool,
                            chan,
                            buffer,
                            source,
                        );
                        // This push should now not fail
                        let _ = buffer.push(e);
                    }
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
            Self::flush_buffer(
                self.sender_id,
                &mut self.buffer_pool,
                channel,
                buffer,
                source,
            );
        }
    }

    // Helper function to reduce duplication of code...
    #[inline(always)]
    fn flush_buffer<CD>(
        sender_id: NodeID,
        buffer_pool: &mut BufferPool<ArconEventWrapper<A>>,
        channel: &Channel<A>,
        writer: &mut BufferWriter<ArconEventWrapper<A>>,
        source: &CD,
    ) where
        CD: ComponentDefinition + Sized + 'static,
    {
        let msg = ArconMessage {
            events: writer.reader(),
            sender: sender_id,
        };
        if let Err(SerError::BufferError(err)) = send(channel, msg, source) {
            // TODO: Figure out how to get more space for `tell_serialised`
            panic!("Buffer Error {}", err);
        };
        // set a new writer
        *writer = buffer_pool.get();
    }

    #[inline]
    pub fn num_channels(&self) -> usize {
        self.buffer_map.len()
    }
}

#[cfg(test)]
mod tests {
    use super::{Channel, *};
    use crate::{
        data::{ArconElement, ArconEvent, NodeID},
        pipeline::Pipeline,
        stream::{
            channel::strategy::{tests::*, ChannelStrategy},
            node::debug::DebugNode,
        },
    };
    use kompact::prelude::*;
    use rand::Rng;
    use std::sync::Arc;

    #[test]
    fn keyby_test() {
        let mut pipeline = Pipeline::default();
        let pool_info = pipeline.get_pool_info();
        let system = pipeline.data_system();

        let parallelism: u32 = 8;
        let total_msgs = 1000;

        let mut channels: Vec<Channel<Input>> = Vec::new();
        let mut comps: Vec<Arc<crate::prelude::Component<DebugNode<Input>>>> = Vec::new();

        for _i in 0..parallelism {
            let comp = system.create(DebugNode::<Input>::new);
            system.start(&comp);
            let actor_ref: ActorRefStrong<ArconMessage<Input>> =
                comp.actor_ref().hold().expect("failed to fetch");
            channels.push(Channel::Local(actor_ref));
            comps.push(comp);
        }

        let max_key = 256;
        let mut channel_strategy =
            ChannelStrategy::Keyed(Keyed::new(max_key, channels, NodeID::new(1), pool_info));

        let mut rng = rand::thread_rng();

        let mut inputs: Vec<ArconEvent<Input>> = Vec::new();
        for _i in 0..total_msgs {
            let input = Input {
                id: rng.gen_range(0, 100000),
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
            comp.on_definition(|cd| {
                assert!(!cd.data.is_empty());
            });
        }
        pipeline.shutdown();
    }
}
