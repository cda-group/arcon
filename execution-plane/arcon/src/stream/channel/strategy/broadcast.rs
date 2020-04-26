// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    buffer::event::{BufferPool, BufferWriter, PoolInfo},
    data::{ArconEvent, ArconEventWrapper, ArconMessage, ArconType, NodeID},
    stream::channel::{strategy::send, Channel},
};

/// A Broadcast strategy for one-to-many message sending
#[allow(dead_code)]
pub struct Broadcast<A>
where
    A: ArconType,
{
    /// A buffer pool of EventBuffer's
    buffer_pool: BufferPool<ArconEventWrapper<A>>,
    /// Vec of Channels that messages are broadcasted to
    channels: Vec<Channel<A>>,
    /// A buffer holding outgoing events
    curr_buffer: BufferWriter<ArconEventWrapper<A>>,
    /// An Identifier that is embedded in each outgoing message
    sender_id: NodeID,
    /// Struct holding information regarding the BufferPool
    pool_info: PoolInfo,
}

impl<A> Broadcast<A>
where
    A: ArconType,
{
    pub fn new(channels: Vec<Channel<A>>, sender_id: NodeID, pool_info: PoolInfo) -> Broadcast<A> {
        assert!(
            channels.len() > 1,
            "Number of Channels must exceed 1 for a Broadcast strategy"
        );
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

        let curr_buffer = buffer_pool
            .try_get()
            .expect("failed to fetch initial buffer");

        Broadcast {
            buffer_pool,
            channels,
            curr_buffer,
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
        for (i, channel) in self.channels.iter().enumerate() {
            if i == self.channels.len() - 1 {
                // This is the last channel, thus we can use curr_buffer
                let reader = self.curr_buffer.reader();
                let msg = ArconMessage {
                    events: reader,
                    sender: self.sender_id,
                };
                send(channel, msg);
            } else {
                // Get a new writer
                let mut writer = self.buffer_pool.get();
                // Copy data from our current writer into the new writer...
                writer.copy_from_writer(&self.curr_buffer);
                let msg = ArconMessage {
                    events: writer.reader(),
                    sender: self.sender_id,
                };
                send(channel, msg);
            }
        }
        // We are finished, set a new BufferWriter to curr_buffer
        // TODO: Should probably not busy wait here..
        self.curr_buffer = self.buffer_pool.get();
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
        prelude::DebugNode,
        stream::channel::strategy::{tests::*, ChannelStrategy},
    };
    use kompact::prelude::*;
    use std::sync::Arc;

    #[test]
    fn broadcast_local_test() {
        let mut pipeline = ArconPipeline::new();
        let pool_info = pipeline.get_pool_info();
        let system = pipeline.system();

        let components: u32 = 8;
        let total_msgs: u64 = 10;

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
            ChannelStrategy::Broadcast(Broadcast::new(channels, NodeID::new(1), pool_info));

        for _i in 0..total_msgs {
            let elem = ArconElement::new(Input { id: 1 });
            // Just assume it is all sent from same comp
            channel_strategy.add(ArconEvent::Element(elem));
        }
        channel_strategy.flush();

        std::thread::sleep(std::time::Duration::from_secs(1));

        // Each of the 8 components should have the same amount of msgs..
        for comp in comps {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.data.len() as u64, total_msgs);
        }
        pipeline.shutdown();
    }
}
