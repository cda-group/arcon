// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    buffer::event::{BufferPool, BufferWriter, PoolInfo},
    data::{ArconEvent, ArconEventWrapper, ArconMessage, ArconType, NodeID},
    stream::channel::{strategy::send, Channel},
};
use kompact::prelude::{ComponentDefinition, SerError};
use std::sync::Arc;

/// A Broadcast strategy for one-to-many message sending
#[allow(dead_code)]
pub struct Broadcast<A>
where
    A: ArconType,
{
    /// A buffer pool of EventBuffer's
    buffer_pool: BufferPool<ArconEventWrapper<A>>,
    /// Vec of Channels that messages are broadcasted to
    channels: Vec<Arc<Channel<A>>>,
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
            channels: channels.into_iter().map(Arc::new).collect(),
            curr_buffer,
            sender_id,
            pool_info,
        }
    }

    #[inline]
    fn push_event(&mut self, event: ArconEvent<A>) -> Option<ArconMessage<A>> {
        self.curr_buffer.push(event.into()).map(|e| {
            let msg = self.message();
            self.curr_buffer.push(e);
            msg
        })
    }
    #[inline]
    fn message(&mut self) -> ArconMessage<A> {
        let reader = self.curr_buffer.reader();
        let msg = ArconMessage {
            events: reader,
            sender: self.sender_id,
        };

        // TODO: Should probably not busy wait here..
        self.curr_buffer = self.buffer_pool.get();

        msg
    }

    #[inline]
    pub fn add(&mut self, event: ArconEvent<A>) -> Vec<(Arc<Channel<A>>, ArconMessage<A>)> {
        match &event {
            ArconEvent::Element(_) => self
                .push_event(event)
                .map(move |msg| {
                    self.channels
                        .iter()
                        .map(|c| (c.clone(), msg.clone()))
                        .collect()
                })
                .unwrap_or_else(Vec::new),
            _ => match self.push_event(event) {
                Some(msg) => {
                    let msg_two = self.message();
                    self.channels
                        .iter()
                        .map(|c| (c.clone(), msg.clone()))
                        .chain(self.channels.iter().map(|c| (c.clone(), msg_two.clone())))
                        .collect()
                }
                None => {
                    let msg = self.message();
                    self.channels
                        .iter()
                        .map(|c| (c.clone(), msg.clone()))
                        .collect()
                }
            },
        }
    }

    #[inline]
    pub fn flush<CD>(&mut self, source: &CD)
    where
        CD: ComponentDefinition + Sized + 'static,
    {
        for (i, channel) in self.channels.iter().enumerate() {
            if i == self.channels.len() - 1 {
                // This is the last channel, thus we can use curr_buffer
                let reader = self.curr_buffer.reader();
                let msg = ArconMessage {
                    events: reader,
                    sender: self.sender_id,
                };
                if let Err(SerError::BufferError(err)) = send(channel, msg, source) {
                    // TODO: Figure out how to get more space for `tell_serialised`
                    panic!("Buffer Error {}", err);
                };
            } else {
                // Get a new writer
                let mut writer = self.buffer_pool.get();
                // Copy data from our current writer into the new writer...
                writer.copy_from_writer(&self.curr_buffer);
                let msg = ArconMessage {
                    events: writer.reader(),
                    sender: self.sender_id,
                };
                if let Err(SerError::BufferError(err)) = send(channel, msg, source) {
                    // TODO: Figure out how to get more space for `tell_serialised`
                    panic!("Buffer Error {}", err);
                };
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
    use super::{Channel, *};
    use crate::{
        application::Application,
        data::{ArconElement, Watermark},
        stream::{
            channel::strategy::{send, tests::*, ChannelStrategy},
            node::debug::DebugNode,
        },
    };
    use kompact::prelude::*;
    use std::sync::Arc;

    #[test]
    fn broadcast_local_test() {
        let mut app = Application::default();
        let pool_info = app.get_pool_info();
        let system = app.data_system();

        let components: u32 = 8;
        let total_msgs: u64 = 10;

        let mut channels: Vec<Channel<Input>> = Vec::new();
        let mut comps: Vec<Arc<crate::prelude::Component<DebugNode<Input>>>> = Vec::new();

        for _i in 0..components {
            let comp = system.create(DebugNode::<Input>::new);
            system.start(&comp);
            let actor_ref: ActorRefStrong<ArconMessage<Input>> =
                comp.actor_ref().hold().expect("failed to fetch");
            channels.push(Channel::Local(actor_ref));
            comps.push(comp);
        }

        let mut channel_strategy: ChannelStrategy<Input> =
            ChannelStrategy::Broadcast(Broadcast::new(channels, NodeID::new(1), pool_info));

        // take one comp as channel source
        // just for testing...
        let comp = &comps[0];
        comp.on_definition(|cd| {
            for _i in 0..total_msgs {
                let elem = ArconElement::new(Input { id: 1 });
                for (channel, msg) in channel_strategy.push(ArconEvent::Element(elem)) {
                    send(&channel, msg, cd).unwrap();
                }
            }
            // force a flush through a marker
            for (channel, msg) in channel_strategy.push(ArconEvent::Watermark(Watermark::new(0))) {
                send(&channel, msg, cd).unwrap();
            }
        });

        std::thread::sleep(std::time::Duration::from_secs(1));

        // Each of the 8 components should have the same amount of msgs..
        for comp in comps {
            comp.on_definition(|cd| {
                assert_eq!(cd.data.len() as u64, total_msgs);
            });
        }
        app.shutdown();
    }
}
