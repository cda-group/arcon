// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::data::{ArconEvent, ArconMessage, ArconType, NodeID};
use crate::prelude::KompactSystem;
use crate::streaming::channel::{strategy::send, strategy::ChannelStrategy, Channel};

pub struct RoundRobin<A>
where
    A: ArconType,
{
    channels: Vec<Channel<A>>,
    sender_id: NodeID,
    curr_index: usize,
    buffer: Vec<ArconEvent<A>>,
}

impl<A> RoundRobin<A>
where
    A: ArconType,
{
    pub fn new(channels: Vec<Channel<A>>, sender_id: NodeID) -> RoundRobin<A> {
        RoundRobin {
            channels,
            sender_id,
            curr_index: 0,
            buffer: Vec::new(),
        }
    }
}

impl<A> ChannelStrategy<A> for RoundRobin<A>
where
    A: ArconType,
{
    fn add(&mut self, event: ArconEvent<A>) {
        self.buffer.push(event);
    }

    fn flush(&mut self, source: &KompactSystem) {
        if let Some(channel) = self.channels.get(self.curr_index) {
            let msg = ArconMessage {
                events: self.buffer.clone(),
                sender: self.sender_id,
            };

            send(&channel, msg, source);

            // TODO: fix this..
            self.buffer.truncate(0);

            self.curr_index += 1;

            if self.curr_index >= self.channels.len() {
                self.curr_index = 0;
            }
        } else {
            panic!("Bad channel setup");
        }
    }

    fn add_and_flush(&mut self, event: ArconEvent<A>, source: &KompactSystem) {
        self.add(event);
        self.flush(source);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::ArconElement;
    use crate::prelude::DebugNode;
    use crate::streaming::channel::strategy::tests::*;
    use kompact::prelude::*;
    use std::sync::Arc;

    #[test]
    fn round_robin_local_test() {
        let system = KompactConfig::default().build().expect("KompactSystem");

        let components: u64 = 8;
        let total_msgs: u64 = components * 4;

        let mut channels: Vec<Channel<Input>> = Vec::new();
        let mut comps: Vec<Arc<crate::prelude::Component<DebugNode<Input>>>> = Vec::new();

        // Create half of the channels using ActorRefs
        for _i in 0..components {
            let comp = system.create_and_start(move || DebugNode::<Input>::new());
            let actor_ref: ActorRefStrong<ArconMessage<Input>> =
                comp.actor_ref().hold().expect("failed to fetch");
            channels.push(Channel::Local(actor_ref));
            comps.push(comp);
        }

        let mut channel_strategy: Box<dyn ChannelStrategy<Input>> =
            Box::new(RoundRobin::new(channels, NodeID::new(1)));

        for _i in 0..total_msgs {
            let elem = ArconElement::new(Input { id: 1 });
            let _ = channel_strategy.add_and_flush(ArconEvent::Element(elem), &system);
        }

        std::thread::sleep(std::time::Duration::from_secs(1));

        for comp in comps {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.data.len() as u64, total_msgs / components);
        }
        let _ = system.shutdown();
    }
}
