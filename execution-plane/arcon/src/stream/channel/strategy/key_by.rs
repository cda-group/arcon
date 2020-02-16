// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::data::{ArconEvent, ArconType};
use crate::prelude::KompactSystem;
use crate::prelude::*;
use crate::stream::channel::strategy::send;
use fnv::FnvHasher;
use std::collections::HashMap;
use std::default::Default;
use std::hash::{BuildHasher, BuildHasherDefault, Hasher};

type DefaultHashBuilder = BuildHasherDefault<FnvHasher>;

/// A hash based partitioner
///
/// `KeyBy` may be constructed with
/// either a custom hasher or the default `FnvHasher`
#[derive(Default)]
pub struct KeyBy<A, H = DefaultHashBuilder>
where
    A: ArconType,
{
    builder: H,
    parallelism: u32,
    sender_id: NodeID,
    buffer_map: HashMap<usize, (Channel<A>, Vec<ArconEvent<A>>)>,
}

impl<A> KeyBy<A>
where
    A: ArconType,
{
    pub fn new(parallelism: u32, channels: Vec<Channel<A>>, sender_id: NodeID) -> KeyBy<A> {
        assert_eq!(channels.len(), parallelism as usize);
        let mut buffer_map: HashMap<usize, (Channel<A>, Vec<ArconEvent<A>>)> = HashMap::new();

        for (i, channel) in channels.into_iter().enumerate() {
            buffer_map.insert(i, (channel, Vec::new()));
        }

        KeyBy {
            builder: Default::default(),
            parallelism,
            sender_id,
            buffer_map,
        }
    }

    pub fn with_hasher<B>(
        parallelism: u32,
        channels: Vec<Channel<A>>,
        sender_id: NodeID,
    ) -> KeyBy<A, BuildHasherDefault<B>>
    where
        B: Hasher + Default,
    {
        assert_eq!(channels.len(), parallelism as usize);
        let mut buffer_map: HashMap<usize, (Channel<A>, Vec<ArconEvent<A>>)> = HashMap::new();

        for (i, channel) in channels.into_iter().enumerate() {
            buffer_map.insert(i, (channel, Vec::new()));
        }
        KeyBy {
            builder: BuildHasherDefault::<B>::default(),
            sender_id,
            parallelism,
            buffer_map,
        }
    }
    pub fn with_batch_size(
        parallelism: u32,
        channels: Vec<Channel<A>>,
        sender_id: NodeID,
        batch_size: usize,
    ) -> KeyBy<A> {
        assert_eq!(channels.len(), parallelism as usize);
        let mut buffer_map: HashMap<usize, (Channel<A>, Vec<ArconEvent<A>>)> = HashMap::new();

        for (i, channel) in channels.into_iter().enumerate() {
            buffer_map.insert(i, (channel, Vec::with_capacity(batch_size)));
        }

        KeyBy {
            builder: Default::default(),
            parallelism,
            sender_id,
            buffer_map,
        }
    }

    pub fn add(&mut self, event: ArconEvent<A>) {
        match &event {
            ArconEvent::Element(element) => {
                if let Some(data) = &element.data {
                    let mut h = self.builder.build_hasher();
                    data.hash(&mut h);
                    let hash = h.finish() as u32;
                    let index = (hash % self.parallelism) as usize;
                    if let Some((_, buffer)) = self.buffer_map.get_mut(&index) {
                        buffer.push(event);
                    } else {
                        panic!("Bad KeyBy setup");
                    }
                }
            }
            _ => {
                // Push watermark/epoch into all outgoing buffers
                for (_, (_, buffer)) in self.buffer_map.iter_mut() {
                    buffer.push(event.clone());
                }
            }
        }
    }

    pub fn flush(&mut self, source: &KompactSystem) {
        let sender_id = self.sender_id;
        for (_, (ref channel, buffer)) in self.buffer_map.iter_mut() {
            let msg = ArconMessage {
                events: buffer.clone(),
                sender: sender_id,
            };
            send(channel, msg, source);
            buffer.clear();
        }
    }

    pub fn add_and_flush(&mut self, event: ArconEvent<A>, source: &KompactSystem) {
        self.add(event);
        self.flush(source);
    }
}

/*

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::ArconEvent;
    use crate::stream::channel::strategy::tests::*;
    use kompact::prelude::*;
    use rand::Rng;
    use std::sync::Arc;

    #[test]
    fn partitioner_parallelism_8_test() {
        let system = KompactConfig::default().build().expect("KompactSystem");

        let parallelism: u32 = 8;
        let total_msgs = 1000;

        let mut channels: Vec<Channel<Input>> = Vec::new();
        let mut comps: Vec<Arc<crate::prelude::Component<DebugNode<Input>>>> = Vec::new();

        for _i in 0..parallelism {
            let comp = system.create(move || DebugNode::<Input>::new());
            system.start(&comp);
            let actor_ref: ActorRefStrong<ArconMessage<Input>> =
                comp.actor_ref().hold().expect("failed to fetch");
            channels.push(Channel::Local(actor_ref));
            comps.push(comp);
        }

        let mut channel_strategy: Box<dyn ChannelStrategy<Input>> =
            Box::new(KeyBy::new(parallelism, channels, NodeID::new(1)));

        let mut rng = rand::thread_rng();

        let mut inputs: Vec<ArconEvent<Input>> = Vec::new();
        for _i in 0..total_msgs {
            let input = Input {
                id: rng.gen_range(0, 100),
            };
            let elem = ArconElement::new(input);
            inputs.push(ArconEvent::Element(elem));
        }

        for input in inputs {
            let _ = channel_strategy.add(input);
        }
        let _ = channel_strategy.flush(&system);

        std::thread::sleep(std::time::Duration::from_secs(1));

        // Each of the 8 components should at least get some hits
        for comp in comps {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert!(comp_inspect.data.len() > 0);
        }
        let _ = system.shutdown();
    }
}
*/
