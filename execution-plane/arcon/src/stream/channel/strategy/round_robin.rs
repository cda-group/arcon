// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    buffer::event::{BufferPool, BufferWriter, PoolInfo},
    data::{ArconEvent, ArconEventWrapper, ArconMessage, ArconType, NodeID},
    stream::channel::{strategy::send, Channel},
};

/// A strategy that sends message downstream in a Round-Robin fashion
pub struct RoundRobin<A>
where
    A: ArconType,
{
    /// A buffer pool of EventBuffer's
    buffer_pool: BufferPool<ArconEventWrapper<A>>,
    /// A buffer holding outgoing events
    curr_buffer: BufferWriter<ArconEventWrapper<A>>,
    /// Vec of Channels
    channels: Vec<Channel<A>>,
    /// An identifier that is embedded with outgoing messages
    sender_id: NodeID,
    /// Struct holding information regarding the BufferPool
    _pool_info: PoolInfo,
    /// Which channel is currently the target
    curr_index: usize,
}

impl<A> RoundRobin<A>
where
    A: ArconType,
{
    /// Creates a RoundRobin strategy
    pub fn new(channels: Vec<Channel<A>>, sender_id: NodeID, pool_info: PoolInfo) -> RoundRobin<A> {
        assert!(
            channels.len() > 1,
            "Number of Channels must exceed 1 for a RoundRobin strategy"
        );

        let mut buffer_pool: BufferPool<ArconEventWrapper<A>> = BufferPool::new(
            pool_info.capacity,
            pool_info.buffer_size,
            pool_info.allocator.clone(),
        )
        .expect("failed to initialise buffer pool");

        let curr_buffer = buffer_pool
            .try_get()
            .expect("failed to fetch initial buffer");

        RoundRobin {
            buffer_pool,
            curr_buffer,
            channels,
            sender_id,
            _pool_info: pool_info,
            curr_index: 0,
        }
    }

    #[inline]
    pub fn add(&mut self, event: ArconEvent<A>) {
        if let ArconEvent::Element(_) = &event {
            if let Some(e) = self.curr_buffer.push(event.into()) {
                // buffer is full, flush.
                self.flush();
                self.curr_buffer.push(e.into());
            }
        } else {
            // Watermark/Epoch.
            // Send downstream as soon as possible

            if let Some(e) = self.curr_buffer.push(event.into()) {
                self.flush();
                self.curr_buffer.push(e.into());
                self.flush();
            } else {
                self.flush();
            }
        }
    }

    #[inline]
    pub fn flush(&mut self) {
        if let Some(channel) = self.channels.get(self.curr_index) {
            let reader = self.curr_buffer.reader();
            let msg = ArconMessage {
                events: reader,
                sender: self.sender_id,
            };
            send(&channel, msg);

            self.curr_index += 1;

            if self.curr_index >= self.channels.len() {
                self.curr_index = 0;
            }

            // TODO: Should probably not busy wait here..
            self.curr_buffer = self.buffer_pool.get();
        } else {
            panic!("Bad channel setup");
        }
    }

    #[inline]
    pub fn num_channels(&self) -> usize {
        self.channels.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data::ArconElement,
        pipeline::ArconPipeline,
        prelude::{ChannelStrategy, DebugNode},
        stream::channel::strategy::tests::*,
    };
    use kompact::prelude::*;
    use std::sync::Arc;

    #[test]
    fn round_robin_local_test() {
        let mut pipeline = ArconPipeline::new();
        let pool_info = pipeline.get_pool_info();
        let system = pipeline.system();

        let components: u64 = 8;
        let total_msgs: u64 = components * 4;

        let mut channels: Vec<Channel<Input>> = Vec::new();
        let mut comps: Vec<Arc<crate::prelude::Component<DebugNode<Input>>>> = Vec::new();

        for _i in 0..components {
            let comp = system.create(move || DebugNode::<Input>::new());
            system.start(&comp);
            let actor_ref: ActorRefStrong<ArconMessage<Input>> =
                comp.actor_ref().hold().expect("failed to fetch");
            channels.push(Channel::Local(actor_ref));
            comps.push(comp);
        }

        let mut channel_strategy: ChannelStrategy<Input> =
            ChannelStrategy::RoundRobin(RoundRobin::new(channels, NodeID::new(1), pool_info));

        for _i in 0..total_msgs {
            let elem = ArconElement::new(Input { id: 1 });
            let _ = channel_strategy.add(ArconEvent::Element(elem));
            channel_strategy.flush();
        }

        std::thread::sleep(std::time::Duration::from_secs(1));

        for comp in comps {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.data.len() as u64, total_msgs / components);
        }

        pipeline.shutdown();
    }
}
