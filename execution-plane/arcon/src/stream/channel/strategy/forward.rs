// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    buffer::event::{BufferPool, BufferWriter, PoolInfo},
    prelude::*,
    stream::channel::{strategy::send, Channel},
};

/// `Forward` is a one-to-one channel strategy between two components
#[allow(dead_code)]
pub struct Forward<A>
where
    A: ArconType,
{
    /// A buffer pool of EventBuffer's
    buffer_pool: BufferPool<ArconEventWrapper<A>>,
    /// A buffer holding outgoing events
    curr_buffer: BufferWriter<ArconEventWrapper<A>>,
    /// Channel that represents a connection to another component
    channel: Channel<A>,
    /// An identifier that is embedded with outgoing messages
    sender_id: NodeID,
    /// Struct holding information regarding the BufferPool
    pool_info: PoolInfo,
}

impl<A> Forward<A>
where
    A: ArconType,
{
    /// Creates a Forward strategy
    pub fn new(channel: Channel<A>, sender_id: NodeID, pool_info: PoolInfo) -> Forward<A> {
        let mut buffer_pool: BufferPool<ArconEventWrapper<A>> = BufferPool::new(
            pool_info.capacity,
            pool_info.buffer_size,
            pool_info.allocator.clone(),
        )
        .expect("failed to initialise buffer pool");

        let curr_buffer = buffer_pool
            .try_get()
            .expect("failed to fetch initial buffer");
        Forward {
            buffer_pool,
            curr_buffer,
            channel,
            sender_id,
            pool_info,
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
            // TODO: bit ugly..

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
        let reader = self.curr_buffer.reader();
        let msg = ArconMessage {
            events: reader,
            sender: self.sender_id,
        };
        send(&self.channel, msg);

        // TODO: Should probably not busy wait here..
        self.curr_buffer = self.buffer_pool.get();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::channel::strategy::{tests::*, ChannelStrategy};
    use kompact::prelude::*;

    #[test]
    fn forward_test() {
        let mut pipeline = ArconPipeline::new();
        let pool_info = pipeline.get_pool_info();
        let system = pipeline.system();

        let total_msgs = 10;
        let comp = system.create(move || DebugNode::<Input>::new());
        system.start(&comp);
        let actor_ref: ActorRefStrong<ArconMessage<Input>> =
            comp.actor_ref().hold().expect("failed to fetch");
        let mut channel_strategy: ChannelStrategy<Input> =
            ChannelStrategy::Forward(Forward::new(Channel::Local(actor_ref), 1.into(), pool_info));

        for _i in 0..total_msgs {
            let elem = ArconElement::new(Input { id: 1 });
            let _ = channel_strategy.add(ArconEvent::Element(elem));
        }
        channel_strategy.flush();

        std::thread::sleep(std::time::Duration::from_secs(1));
        {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.data.len(), total_msgs);
        }
        let _ = pipeline.shutdown();
    }
}
