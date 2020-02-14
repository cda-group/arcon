// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

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
}

impl<A> Forward<A>
where
    A: ArconType,
{
    /// Creates a Forward strategy
    ///
    /// `Forward::new` will allocate on demand. Should be used for testing and development only.
    pub fn new(channel: Channel<A>, sender_id: NodeID) -> Forward<A> {
        Forward {
            channel,
            sender_id,
            buffer: Vec::new(),
        }
    }

    /// Creates a Forward strategy
    ///
    /// `Forward::with_batch_size` will preallocate its buffer according to `batch_size`
    pub fn with_batch_size(
        channel: Channel<A>,
        sender_id: NodeID,
        batch_size: usize,
    ) -> Forward<A> {
        Forward {
            channel,
            sender_id,
            buffer: Vec::with_capacity(batch_size),
        }
    }
    pub fn add(&mut self, event: ArconEvent<A>) {
        self.buffer.push(event);
    }

    pub fn flush(&mut self, source: &KompactSystem) {
        let msg = ArconMessage {
            events: self.buffer.clone(),
            sender: self.sender_id,
        };

        send(&self.channel, msg, source);
        self.buffer.clear();
    }

    pub fn add_and_flush(&mut self, event: ArconEvent<A>, source: &KompactSystem) {
        self.add(event);
        self.flush(source);
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
            let _ = channel_strategy.add_and_flush(ArconEvent::Element(elem), &system);
        }

        std::thread::sleep(std::time::Duration::from_secs(1));
        {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.data.len(), total_msgs);
        }
        let _ = system.shutdown();
    }
}
