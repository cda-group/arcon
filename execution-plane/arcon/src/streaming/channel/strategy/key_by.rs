use crate::streaming::channel::strategy::channel_output;
use crate::data::{ArconEvent, ArconType};
use crate::prelude::*;
use fnv::FnvHasher;
use kompact::KompactSystem;
use std::collections::HashMap;
use std::default::Default;
use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};
use std::marker::PhantomData;

/// A hash based partitioner
///
/// `KeyBy` may be constructed with
/// either a custom hasher or the default `FnvHasher`
pub struct KeyBy<A, D = BuildHasherDefault<FnvHasher>>
where
    A: 'static + ArconType + Hash,
{
    builder: D,
    parallelism: u32,
    map: HashMap<usize, Channel, D>,
    phantom_a: PhantomData<A>,
}

impl<A> KeyBy<A>
where
    A: 'static + ArconType + Hash,
{
    pub fn with_hasher<H: BuildHasher + Default>(
        builder: H,
        parallelism: u32,
        channels: Vec<Channel>,
    ) -> KeyBy<A, H> {
        assert_eq!(channels.len(), parallelism as usize);
        let mut map = HashMap::with_capacity_and_hasher(parallelism as usize, Default::default());
        for (i, channel) in channels.into_iter().enumerate() {
            map.insert(i, channel);
        }
        KeyBy {
            builder: builder.into(),
            parallelism,
            map,
            phantom_a: PhantomData,
        }
    }

    pub fn with_default_hasher(parallelism: u32, channels: Vec<Channel>) -> Self {
        assert_eq!(channels.len(), parallelism as usize);
        let mut map = HashMap::with_capacity_and_hasher(parallelism as usize, Default::default());
        for (i, channel) in channels.into_iter().enumerate() {
            map.insert(i, channel);
        }
        KeyBy {
            builder: BuildHasherDefault::<FnvHasher>::default(),
            parallelism,
            map,
            phantom_a: PhantomData,
        }
    }
}

impl<A> ChannelStrategy<A> for KeyBy<A>
where
    A: 'static + ArconType + Hash,
{
    fn output(&mut self, message: ArconMessage<A>, source: &KompactSystem) -> ArconResult<()> {
        match message.event {
            ArconEvent::Element(element) => {
                let mut h = self.builder.build_hasher();
                element.data.hash(&mut h);
                let hash = h.finish() as u32;
                let id = (hash % self.parallelism) as i32;
                if id >= 0 && id <= self.parallelism as i32 {
                    if let Some(channel) = self.map.get(&(id as usize)) {
                        let _ = channel_output(channel, message, source)?;
                    }
                } else {
                    // TODO: Fix
                    panic!("Failed to hash to channel properly..");
                }
            }
            _ => {
                for (_, channel) in self.map.iter() {
                    let _ = channel_output(channel, message.clone(), source)?;
                }
            }
        }
        Ok(())
    }

    fn add_channel(&mut self, _channel: Channel) {
        unimplemented!();
    }
    fn remove_channel(&mut self, _channel: Channel) {
        unimplemented!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::channel::strategy::tests::*;
    use kompact::*;
    use rand::Rng;
    use std::sync::Arc;

    #[test]
    fn partitioner_parallelism_8_test() {
        let cfg = KompactConfig::new();
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let parallelism: u32 = 8;
        let total_msgs = 1000;

        let mut channels: Vec<Channel> = Vec::new();
        let mut comps: Vec<Arc<crate::prelude::Component<DebugSink<Input>>>> = Vec::new();

        for _i in 0..parallelism {
            let comp = system.create_and_start(move || DebugSink::<Input>::new());
            channels.push(Channel::Local(comp.actor_ref()));
            comps.push(comp);
        }

        let mut channel_strategy: Box<ChannelStrategy<Input>> =
            Box::new(KeyBy::with_default_hasher(parallelism, channels));

        let mut rng = rand::thread_rng();

        let mut inputs: Vec<ArconMessage<Input>> = Vec::new();
        for _i in 0..total_msgs {
            let input = Input {
                id: rng.gen_range(0, 100),
            };
            inputs.push(ArconMessage::element(input, None, "test".to_string()));
        }

        for input in inputs {
            // Just assume it is all sent from same comp
            let _ = channel_strategy.output(input, &system);
        }

        std::thread::sleep(std::time::Duration::from_secs(1));

        // Each of the 8 components should at least get some hits
        for comp in comps {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert!(comp_inspect.data.len() > 0);
        }
        let _ = system.shutdown();
    }
}
