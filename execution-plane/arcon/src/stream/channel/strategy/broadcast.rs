// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::data::{ArconEvent, ArconMessage, ArconType, NodeID};
use crate::prelude::KompactSystem;
use crate::stream::channel::{strategy::send, strategy::ChannelStrategy, Channel};

pub struct Broadcast<A>
where
    A: ArconType,
{
    channels: Vec<Channel<A>>,
    sender_id: NodeID,
    buffer: Vec<ArconEvent<A>>,
}

impl<A> Broadcast<A>
where
    A: ArconType,
{
    pub fn new(channels: Vec<Channel<A>>, sender_id: NodeID) -> Broadcast<A> {
        Broadcast {
            channels,
            sender_id,
            buffer: Vec::new(),
        }
    }
}

impl<A> ChannelStrategy<A> for Broadcast<A>
where
    A: 'static + ArconType,
{
    fn add(&mut self, event: ArconEvent<A>) {
        self.buffer.push(event);
    }

    fn flush(&mut self, source: &KompactSystem) {
        let msg = ArconMessage {
            events: self.buffer.clone(),
            sender: self.sender_id,
        };

        for channel in &self.channels {
            send(channel, msg.clone(), source);
        }

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
    use crate::data::ArconElement;
    use crate::prelude::DebugNode;
    use crate::stream::channel::strategy::tests::*;
    use crate::stream::channel::ArconSerde;
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

        let mut channel_strategy: Box<dyn ChannelStrategy<Input>> =
            Box::new(Broadcast::new(channels, NodeID::new(1)));

        for _i in 0..total_msgs {
            let elem = ArconElement::new(Input { id: 1 });
            // Just assume it is all sent from same comp
            channel_strategy.add_and_flush(ArconEvent::Element(elem), &system);
        }

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

            let remote_path = ActorPath::Named(NamedPath::with_system(
                remote.system_path(),
                vec![comp_id.into()],
            ));
            channels.push(Channel::Remote((remote_path, ArconSerde::default())));
            comps.push(comp);
        }
        std::thread::sleep(std::time::Duration::from_secs(1));

        let mut channel_strategy: Box<dyn ChannelStrategy<Input>> =
            Box::new(Broadcast::new(channels, NodeID::new(1)));

        for _i in 0..total_msgs {
            let elem = ArconElement::new(Input { id: 1 });
            // Just assume it is all sent from same comp
            channel_strategy.add_and_flush(ArconEvent::Element(elem), &system);
        }

        std::thread::sleep(std::time::Duration::from_secs(1));

        // Each of the 8 components should have same amount of msgs..
        for comp in comps {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.data.len() as u64, total_msgs);
        }
        let _ = system.shutdown();
    }
}
