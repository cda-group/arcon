// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use super::DEFAULT_BATCH_SIZE;
use crate::prelude::*;
use crate::stream::channel::{strategy::send, Channel};

/// `Forward` is a one-to-one channel strategy between two components
pub struct Forward<A>
where
    A: ArconType,
{
    /// Channel that represents a connection to another component
    channel: Channel<A>,
    /// An identifier that is embedded with outgoing messages
    sender_id: NodeID,
    /// A buffer holding outgoing events
    buffer: Vec<ArconEvent<A>>,
    /// A batch size indicating when the channel should flush data
    batch_size: usize,
    /// A counter keeping track of how many events there are in the buffer
    buffer_counter: usize,
}

impl<A> Forward<A>
where
    A: ArconType,
{
    /// Creates a Forward strategy
    ///
    /// `Forward::new` will utilise [DEFAULT_BATCH_SIZE] as batch size
    pub fn new(channel: Channel<A>, sender_id: NodeID) -> Forward<A> {
        Forward {
            channel,
            sender_id,
            buffer: Vec::with_capacity(DEFAULT_BATCH_SIZE),
            batch_size: DEFAULT_BATCH_SIZE,
            buffer_counter: 0,
        }
    }

    /// Creates a Forward strategy
    ///
    /// `Forward::with_batch_size` will preallocate its buffer according to a custom batch size
    pub fn with_batch_size(
        channel: Channel<A>,
        sender_id: NodeID,
        batch_size: usize,
    ) -> Forward<A> {
        Forward {
            channel,
            sender_id,
            buffer: Vec::with_capacity(batch_size),
            batch_size,
            buffer_counter: 0,
        }
    }

    #[inline]
    pub fn add(&mut self, event: ArconEvent<A>) {
        if let ArconEvent::Element(_) = &event {
            self.buffer.push(event);
            self.buffer_counter += 1;

            if self.buffer_counter == self.batch_size {
                self.flush();
            }
        } else {
            // Watermark/Epoch.
            // Send downstream as soon as possible
            self.buffer.push(event);
            self.flush();
        }
    }

    #[inline]
    pub fn flush(&mut self) {
        let mut new_vec = Vec::with_capacity(self.batch_size);
        std::mem::swap(&mut new_vec, &mut self.buffer);
        let msg = ArconMessage {
            events: new_vec,
            sender: self.sender_id,
        };

        send(&self.channel, msg);
        self.buffer_counter = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::channel::strategy::tests::*;
    use crate::stream::channel::strategy::ChannelStrategy;
    use kompact::prelude::*;

    #[test]
    fn forward_test() {
        let system = KompactConfig::default().build().expect("KompactSystem");

        let total_msgs = 10;
        let comp = system.create(move || DebugNode::<Input>::new());
        system.start(&comp);
        let actor_ref: ActorRefStrong<ArconMessage<Input>> =
            comp.actor_ref().hold().expect("failed to fetch");
        let mut channel_strategy: ChannelStrategy<Input> =
            ChannelStrategy::Forward(Forward::new(Channel::Local(actor_ref), 1.into()));

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
        let _ = system.shutdown();
    }
}
