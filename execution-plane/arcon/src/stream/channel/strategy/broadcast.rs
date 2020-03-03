// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use super::DEFAULT_BATCH_SIZE;
use crate::{
    data::{ArconEvent, ArconEventProstMessage, ArconMessage, ArconType, NodeID},
    stream::channel::{strategy::send, Channel},
};

/// A Broadcast strategy for one-to-many message sending
pub struct Broadcast<A>
where
    A: ArconType,
{
    /// Vec of Channels that messages are broadcasted to
    channels: Vec<Channel<A>>,
    /// An Identifier that is embedded in each outgoing message
    sender_id: NodeID,
    /// A buffer holding outgoing events
    buffer: Vec<ArconEventProstMessage<A>>,
    /// A batch size indicating when the channel should flush data
    batch_size: usize,
}

impl<A> Broadcast<A>
where
    A: ArconType,
{
    pub fn new(channels: Vec<Channel<A>>, sender_id: NodeID) -> Broadcast<A> {
        Broadcast {
            channels,
            sender_id,
            buffer: Vec::with_capacity(DEFAULT_BATCH_SIZE),
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }
    pub fn with_batch_size(
        channels: Vec<Channel<A>>,
        sender_id: NodeID,
        batch_size: usize,
    ) -> Broadcast<A> {
        Broadcast {
            channels,
            sender_id,
            buffer: Vec::with_capacity(batch_size),
            batch_size,
        }
    }

    #[inline]
    pub fn add(&mut self, event: ArconEvent<A>) {
        if let ArconEvent::Element(_) = &event {
            self.buffer.push(event.into());

            if self.buffer.len() == self.batch_size {
                self.flush();
            }
        } else {
            // Watermark/Epoch.
            // Send downstream as soon as possible
            self.buffer.push(event.into());
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

        for channel in &self.channels {
            send(channel, msg.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data::ArconElement,
        prelude::DebugNode,
        stream::channel::{
            strategy::{tests::*, ChannelStrategy},
            FlightSerde,
        },
    };
    use kompact::prelude::*;
    use std::sync::Arc;

    #[test]
    fn broadcast_local_test() {
        let system = KompactConfig::default().build().expect("KompactSystem");

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
            ChannelStrategy::Broadcast(Broadcast::new(channels, NodeID::new(1)));

        for _i in 0..total_msgs {
            let elem = ArconElement::new(Input { id: 1 });
            // Just assume it is all sent from same comp
            channel_strategy.add(ArconEvent::Element(elem));
        }
        channel_strategy.flush();

        std::thread::sleep(std::time::Duration::from_secs(1));

        // Each of the 8 components should havesame amount of msgs..
        for comp in comps {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.data.len() as u64, total_msgs);
        }
        let _ = system.shutdown();
    }

    #[test]
    fn broadcast_local_and_remote() {
        let (system, remote) = {
            let system = || {
                let mut cfg = KompactConfig::new();
                cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
                cfg.build().expect("KompactSystem")
            };
            (system(), system())
        };

        let local_components: u32 = 4;
        let remote_components: u32 = 4;
        let total_msgs: u64 = 5;

        let mut channels: Vec<Channel<Input>> = Vec::new();
        let mut comps: Vec<Arc<crate::prelude::Component<DebugNode<Input>>>> = Vec::new();

        // Create local components
        for _i in 0..local_components {
            let comp = system.create(move || DebugNode::<Input>::new());
            system.start(&comp);
            let actor_ref: ActorRefStrong<ArconMessage<Input>> =
                comp.actor_ref().hold().expect("failed to fetch");
            channels.push(Channel::Local(actor_ref));
            comps.push(comp);
        }

        // Create remote components
        for i in 0..remote_components {
            let comp = remote.create(move || DebugNode::<Input>::new());
            let comp_id = format!("comp_{}", i);
            let _ = remote.register_by_alias(&comp, comp_id.clone());
            remote.start(&comp);

            let remote_path = ActorPath::Named(NamedPath::with_system(remote.system_path(), vec![
                comp_id.into(),
            ]));
            channels.push(Channel::Remote(
                remote_path,
                FlightSerde::Reliable,
                system.dispatcher_ref().into(),
            ));
            comps.push(comp);
        }
        std::thread::sleep(std::time::Duration::from_secs(1));

        let mut channel_strategy: ChannelStrategy<Input> =
            ChannelStrategy::Broadcast(Broadcast::new(channels, NodeID::new(1)));

        for _i in 0..total_msgs {
            let elem = ArconElement::new(Input { id: 1 });
            // Just assume it is all sent from same comp
            channel_strategy.add(ArconEvent::Element(elem));
        }
        channel_strategy.flush();

        std::thread::sleep(std::time::Duration::from_secs(1));

        // Each of the 8 components should have same amount of msgs..
        for comp in comps {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.data.len() as u64, total_msgs);
        }
        let _ = system.shutdown();
    }
}
