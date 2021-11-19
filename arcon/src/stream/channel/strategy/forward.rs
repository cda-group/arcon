use crate::{
    buffer::event::{BufferPool, BufferWriter, PoolInfo},
    data::{ArconEvent, ArconEventWrapper, ArconMessage, ArconType, NodeID},
    stream::channel::Channel,
};
use std::sync::Arc;

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
    channel: Arc<Channel<A>>,
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
            channel: Arc::new(channel),
            sender_id,
            pool_info,
        }
    }
    pub fn add(&mut self, event: ArconEvent<A>) -> Vec<(Arc<Channel<A>>, ArconMessage<A>)> {
        match &event {
            ArconEvent::Element(_) => {
                // Return message only if buffer is full
                self.push_event(event)
                    .map(move |msg| vec![(self.channel.clone(), msg)])
                    .unwrap_or_else(Vec::new)
            }
            _ => {
                // We received a marker, return messages
                match self.push_event(event) {
                    Some(msg) => {
                        // buffer was full, dispatch returned msg + newly created one with the marker
                        let msg2 = self.message();
                        vec![(self.channel.clone(), msg), (self.channel.clone(), msg2)]
                    }
                    None => {
                        // Buffer was not full, but fetch message and return..
                        let msg = self.message();
                        vec![(self.channel.clone(), msg)]
                    }
                }
            }
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
}

#[cfg(test)]
mod tests {
    use super::{Channel, *};
    use crate::{
        application::AssembledApplication,
        data::{ArconElement, ArconEvent, Watermark},
        stream::{
            channel::strategy::{forward::Forward, send, tests::*, ChannelStrategy},
            node::debug::DebugNode,
        },
    };
    use kompact::prelude::*;

    #[test]
    fn forward_test() {
        let mut app = AssembledApplication::default();
        let pool_info = app.app.get_pool_info();
        let system = app.data_system();

        let total_msgs = 10;
        let comp = system.create(DebugNode::<Input>::new);
        system.start(&comp);
        let actor_ref: ActorRefStrong<ArconMessage<Input>> =
            comp.actor_ref().hold().expect("failed to fetch");
        let mut channel_strategy: ChannelStrategy<Input> =
            ChannelStrategy::Forward(Forward::new(Channel::Local(actor_ref), 1.into(), pool_info));

        comp.on_definition(|cd| {
            for _i in 0..total_msgs {
                let elem = ArconElement::new(Input { id: 1 });
                let _ = channel_strategy.push(ArconEvent::Element(elem));
            }
            // force a flush through a marker
            for (channel, msg) in channel_strategy.push(ArconEvent::Watermark(Watermark::new(0))) {
                let _ = send(&channel, msg, cd);
            }
        });

        std::thread::sleep(std::time::Duration::from_secs(1));
        comp.on_definition(|cd| {
            assert_eq!(cd.data.len() as u64, total_msgs);
        });
        let _ = app.shutdown();
    }
}
