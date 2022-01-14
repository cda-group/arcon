use crate::{
    buffer::event::{BufferPool, BufferWriter, PoolInfo},
    data::{
        partition::shard_lookup_with_key, ArconEvent, ArconEventWrapper, ArconMessage, ArconType,
        NodeID,
    },
    dataflow::stream::KeyBuilder,
    stream::channel::Channel,
};
use std::sync::Arc;

/// A Channel Strategy for Keyed Data Streams
///
/// Data is split onto a contiguous key space containing N key ranges.
pub struct Keyed<A>
where
    A: ArconType,
{
    /// A buffer pool of EventBuffer's
    buffer_pool: BufferPool<ArconEventWrapper<A>>,
    /// Number of ranges on the contiguous key space
    key_ranges: u64,
    /// An identifier that is embedded with outgoing messages
    sender_id: NodeID,
    /// Extract the Key from A
    key_builder: KeyBuilder<A>,
    buffers: Vec<BufferWriter<ArconEventWrapper<A>>>,
    channels: Vec<Arc<Channel<A>>>,
    /// Struct holding information regarding the BufferPool
    _pool_info: PoolInfo,
}

impl<A> Keyed<A>
where
    A: ArconType,
{
    /// Creates a Keyed strategy
    pub fn new(
        channels: Vec<Channel<A>>,
        sender_id: NodeID,
        pool_info: PoolInfo,
        key_builder: KeyBuilder<A>,
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

        let mut buffers = Vec::with_capacity(channels.len());
        for _ in 0..channels.len() {
            let writer = buffer_pool
                .try_get()
                .expect("failed to fetch initial buffer");
            buffers.push(writer)
        }
        Keyed {
            buffer_pool,
            key_ranges: channels_len,
            sender_id,
            channels: channels.into_iter().map(Arc::new).collect::<Vec<_>>(),
            buffers,
            _pool_info: pool_info,
            key_builder,
        }
    }
    #[inline]
    fn push_event(&mut self, index: usize, event: ArconEvent<A>) -> Option<ArconMessage<A>> {
        let writer = &mut self.buffers[index];
        match writer.push(event.into()) {
            Some(e) => {
                let msg = ArconMessage {
                    events: writer.reader(),
                    sender: self.sender_id,
                };
                // set a new writer
                *writer = self.buffer_pool.get();

                // now insert it with fresh buffer writer
                writer.push(e);
                Some(msg)
            }
            None => None,
        }
    }

    #[inline]
    pub fn add(&mut self, event: ArconEvent<A>) -> Vec<(Arc<Channel<A>>, ArconMessage<A>)> {
        match &event {
            ArconEvent::Element(e) => {
                // Get key placement
                let key = self.key_builder.get_key(&e.data);
                // Calculate which key range index is responsible for this key
                let index = shard_lookup_with_key(key, self.key_ranges);

                self.push_event(index as usize, event)
                    .map(move |msg| vec![(self.channels[index as usize].clone(), msg)])
                    .unwrap_or_else(Vec::new)
            }
            _ => {
                let mut outputs = Vec::with_capacity(self.buffers.len());
                // clear all buffers
                for index in 0..self.buffers.len() {
                    match self.push_event(index, event.clone()) {
                        Some(msg) => {
                            // buffer was full
                            let writer = &mut self.buffers[index];
                            let msg_two = ArconMessage {
                                events: writer.reader(),
                                sender: self.sender_id,
                            };
                            // set a new writer
                            *writer = self.buffer_pool.get();

                            outputs.push((self.channels[index].clone(), msg));
                            outputs.push((self.channels[index].clone(), msg_two));
                        }
                        None => {
                            let writer = &mut self.buffers[index];
                            let msg = ArconMessage {
                                events: writer.reader(),
                                sender: self.sender_id,
                            };
                            // set a new writer
                            *writer = self.buffer_pool.get();

                            outputs.push((self.channels[index].clone(), msg));
                        }
                    }
                }
                outputs
            }
        }
    }

    #[inline]
    pub fn num_channels(&self) -> usize {
        self.channels.len()
    }
}

#[cfg(test)]
mod tests {
    use super::{Channel, *};
    use crate::{
        application::assembled::AssembledApplication,
        data::{ArconElement, ArconEvent, NodeID, Watermark},
        stream::{
            channel::strategy::{send, tests::*, ChannelStrategy},
            node::debug::DebugNode,
        },
    };
    use kompact::prelude::*;
    use rand::Rng;
    use std::sync::Arc;

    #[test]
    fn keyby_test() {
        let app = AssembledApplication::default();
        let pool_info = app.app.get_pool_info();
        let system = app.data_system();

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

        let key_builder = KeyBuilder::<Input> {
            extractor: Arc::new(|i: &Input| i.id as u64),
        };

        let mut channel_strategy =
            ChannelStrategy::Keyed(Keyed::new(channels, NodeID::new(1), pool_info, key_builder));

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
                let _ = channel_strategy.push(input);
            }
            // force a flush through a marker
            for (channel, msg) in channel_strategy.push(ArconEvent::Watermark(Watermark::new(0))) {
                let _ = send(&channel, msg, cd);
            }
        });

        std::thread::sleep(std::time::Duration::from_secs(1));

        // Each of the 8 components should at least get some hits
        for comp in comps {
            comp.on_definition(|cd| {
                assert!(!cd.data.is_empty());
            });
        }
        app.shutdown();
    }
}
