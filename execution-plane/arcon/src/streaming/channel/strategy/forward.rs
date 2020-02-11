// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::prelude::*;
use crate::streaming::channel::{strategy::send, strategy::ChannelStrategy, Channel};

pub struct Forward<A>
where
    A: ArconType,
{
    channel: Channel<A>,
    sender_id: NodeID,
    buffer: Vec<ArconEvent<A>>,
}

impl<A> Forward<A>
where
    A: ArconType,
{
    pub fn new(channel: Channel<A>, sender_id: NodeID) -> Forward<A> {
        Forward {
            channel,
            sender_id,
            buffer: Vec::new(),
        }
    }
}

impl<A> ChannelStrategy<A> for Forward<A>
where
    A: ArconType,
{
    fn add(&mut self, event: ArconEvent<A>) {
        self.buffer.push(event);
    }

    fn flush(&mut self, source: &KompactSystem) {
        let msg = ArconMessage {
            events: self.buffer.clone(),
            sender: self.sender_id,
        };

        send(&self.channel, msg, source);
        // TODO: fix this..
        self.buffer.truncate(0);
    }

    fn add_and_flush(&mut self, event: ArconEvent<A>, source: &KompactSystem) {
        self.add(event);
        self.flush(source);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::channel::strategy::tests::*;
    use kompact::prelude::*;

    #[test]
    fn forward_test() {
        let system = KompactConfig::default().build().expect("KompactSystem");

        let total_msgs = 10;
        let comp = system.create_and_start(move || DebugNode::<Input>::new());
        let actor_ref: ActorRefStrong<ArconMessage<Input>> =
            comp.actor_ref().hold().expect("failed to fetch");
        let mut channel_strategy: Box<dyn ChannelStrategy<Input>> =
            Box::new(Forward::new(Channel::Local(actor_ref), 1.into()));

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
