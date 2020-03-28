// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconEvent, ArconType},
    prelude::*,
    stream::channel::strategy::send,
};
use std::{
    collections::{hash_map::DefaultHasher, HashMap},
    default::Default,
    hash::{BuildHasher, BuildHasherDefault, Hasher},
};

type DefaultHashBuilder = BuildHasherDefault<DefaultHasher>;

/// A hash based partitioner
///
/// KeyBy may be constructed with
/// either a custom hasher or the default [DefaultHasher]
#[derive(Clone, Default)]
pub struct KeyBy<A, H = DefaultHashBuilder>
where
    A: ArconType,
{
    /// The HashBuilder
    builder: H,
    /// Used to hash % modulo
    parallelism: u32,
    /// An identifier that is embedded with outgoing messages
    sender_id: NodeID,
    /// A map with hashed indexes and their respective Channel/Buffer
    buffer_map: HashMap<usize, (Channel<A>, Vec<ArconEventWrapper<A>>)>,
    /// A batch size indicating when the channel should flush data
    batch_size: usize,
    /// A counter keeping track of buffered elements across all channels
    buffer_counter: usize,
}

impl<A> KeyBy<A>
where
    A: ArconType,
{
    /// Creates a KeyBy strategy with Rust's default hasher
    pub fn new(
        parallelism: u32,
        channels: Vec<Channel<A>>,
        sender_id: NodeID,
        batch_size: usize,
    ) -> KeyBy<A> {
        assert_eq!(channels.len(), parallelism as usize);
        let mut buffer_map = HashMap::new();

        for (i, channel) in channels.into_iter().enumerate() {
            buffer_map.insert(i, (channel, Vec::with_capacity(batch_size)));
        }

        KeyBy {
            builder: Default::default(),
            parallelism,
            sender_id,
            buffer_map,
            batch_size,
            buffer_counter: 0,
        }
    }

    /// Creates a KeyBy strategy with a custom built [Hasher]
    pub fn with_hasher<B>(
        parallelism: u32,
        channels: Vec<Channel<A>>,
        batch_size: usize,
        sender_id: NodeID,
    ) -> KeyBy<A, BuildHasherDefault<B>>
    where
        B: Hasher + Default,
    {
        assert_eq!(channels.len(), parallelism as usize);
        let mut buffer_map: HashMap<usize, (Channel<A>, Vec<ArconEventWrapper<A>>)> =
            HashMap::new();

        for (i, channel) in channels.into_iter().enumerate() {
            buffer_map.insert(i, (channel, Vec::with_capacity(batch_size)));
        }
        KeyBy {
            builder: BuildHasherDefault::<B>::default(),
            sender_id,
            parallelism,
            buffer_map,
            batch_size,
            buffer_counter: 0,
        }
    }

    #[inline]
    pub fn add(&mut self, event: ArconEvent<A>) {
        match &event {
            ArconEvent::Element(element) => {
                if let Some(data) = &element.data {
                    let mut h = self.builder.build_hasher();
                    data.hash(&mut h);
                    let hash = h.finish() as u32;
                    let index = (hash % self.parallelism) as usize;
                    if let Some((_, buffer)) = self.buffer_map.get_mut(&index) {
                        buffer.push(event.into());
                        self.buffer_counter += 1;
                    } else {
                        panic!("Bad KeyBy setup");
                    }
                    if self.buffer_counter == self.batch_size {
                        self.flush();
                    }
                }
            }
            _ => {
                // Push watermark/epoch into all outgoing buffers
                for (_, (_, buffer)) in self.buffer_map.iter_mut() {
                    buffer.push(event.clone().into());
                }
                self.flush();
            }
        }
    }

    #[inline]
    pub fn flush(&mut self) {
        for (_, (ref channel, buffer)) in self.buffer_map.iter_mut() {
            let mut new_vec = Vec::with_capacity(buffer.len());
            std::mem::swap(&mut new_vec, &mut *buffer);
            let msg = ArconMessage {
                events: new_vec,
                sender: self.sender_id,
            };
            send(channel, msg);
        }

        self.buffer_counter = 0;
    }

    #[inline]
    pub fn num_channels(&self) -> usize {
        self.buffer_map.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{data::ArconEvent, stream::channel::strategy::tests::*};
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

        let mut channel_strategy =
            ChannelStrategy::KeyBy(KeyBy::new(parallelism, channels, NodeID::new(1), 248));

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
        let _ = channel_strategy.flush();

        std::thread::sleep(std::time::Duration::from_secs(1));

        // Each of the 8 components should at least get some hits
        for comp in comps {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert!(comp_inspect.data.len() > 0);
        }
        let _ = system.shutdown();
    }
}
