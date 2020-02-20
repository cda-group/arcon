// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::data::{ArconEvent, ArconMessage, ArconType, NodeID};
use crate::stream::channel::{strategy::send, Channel};

pub struct RoundRobin<A>
where
    A: ArconType,
{
    channels: Vec<Channel<A>>,
    sender_id: NodeID,
    curr_index: usize,
    buffer: Vec<ArconEvent<A>>,
    /// A batch size indicating when the channel should flush data
    batch_size: usize,
    /// A counter keeping track of how many events there are in the buffer
    buffer_counter: usize,
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
            batch_size: 1024,
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
        if let Some(channel) = self.channels.get(self.curr_index) {
            let mut new_vec = Vec::with_capacity(self.batch_size);
            std::mem::swap(&mut new_vec, &mut self.buffer);
            let msg = ArconMessage {
                events: new_vec,
                sender: self.sender_id,
            };

            send(&channel, msg);
            self.buffer_counter = 0;

            self.curr_index += 1;

            if self.curr_index >= self.channels.len() {
                self.curr_index = 0;
            }
        } else {
            panic!("Bad channel setup");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::ArconElement;
    use crate::prelude::{ChannelStrategy, DebugNode};
    use crate::stream::channel::strategy::tests::*;
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
            let comp = system.create(move || DebugNode::<Input>::new());
            system.start(&comp);
            let actor_ref: ActorRefStrong<ArconMessage<Input>> =
                comp.actor_ref().hold().expect("failed to fetch");
            channels.push(Channel::Local(actor_ref));
            comps.push(comp);
        }

        let mut channel_strategy: ChannelStrategy<Input> =
            ChannelStrategy::RoundRobin(RoundRobin::new(channels, NodeID::new(1)));

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
        let _ = system.shutdown();
    }
}
