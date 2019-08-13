use crate::data::{ArconEvent, ArconType};
use crate::error::*;
use crate::streaming::channel::strategy::{channel_output, ChannelStrategy};
use crate::streaming::channel::Channel;
use fnv::FnvHasher;
use kompact::ComponentDefinition;
use std::collections::HashMap;
use std::default::Default;
use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};
use std::marker::PhantomData;

/// A hash based partitioner
///
/// `KeyBy` may be constructed with
/// either a custom hasher or the default `FnvHasher`
pub struct KeyBy<A, B, D = BuildHasherDefault<FnvHasher>>
where
    A: 'static + ArconType + Hash,
    B: ComponentDefinition + Sized + 'static,
{
    builder: D,
    parallelism: u32,
    map: HashMap<usize, Channel, D>,
    phantom_a: PhantomData<A>,
    phantom_b: PhantomData<B>,
}

impl<A, B> KeyBy<A, B>
where
    A: 'static + ArconType + Hash,
    B: ComponentDefinition + Sized + 'static,
{
    pub fn with_hasher<H: BuildHasher + Default>(
        builder: H,
        parallelism: u32,
        channels: Vec<Channel>,
    ) -> KeyBy<A, B, H> {
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
            phantom_b: PhantomData,
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
            phantom_b: PhantomData,
        }
    }
}

impl<A, B> ChannelStrategy<A, B> for KeyBy<A, B>
where
    A: 'static + ArconType + Hash,
    B: ComponentDefinition + Sized + 'static,
{
    fn output(&mut self, event: ArconEvent<A>, source: *const B) -> ArconResult<()> {
        match event {
            ArconEvent::Element(element) => {
                let mut h = self.builder.build_hasher();
                element.data.hash(&mut h);
                let hash = h.finish() as u32;
                let id = (hash % self.parallelism) as i32;
                if id >= 0 && id <= self.parallelism as i32 {
                    if let Some(channel) = self.map.get(&(id as usize)) {
                        let _ = channel_output(channel, event, source)?;
                    }
                } else {
                    // TODO: Fix
                    panic!("Failed to hash to channel properly..");
                }
            }
            ArconEvent::Watermark(_) => {
                for (_, channel) in self.map.iter() {
                    let _ = channel_output(channel, event, source)?;
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

unsafe impl<A, B> Send for KeyBy<A, B>
where
    A: 'static + ArconType + Hash,
    B: ComponentDefinition + Sized + 'static,
{
}

unsafe impl<A, B> Sync for KeyBy<A, B>
where
    A: 'static + ArconType + Hash,
    B: ComponentDefinition + Sized + 'static,
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::ArconElement;
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
        let mut comps: Vec<Arc<crate::prelude::Component<TestComp>>> = Vec::new();

        for _i in 0..parallelism {
            let comp = system.create_and_start(move || TestComp::new());
            channels.push(Channel::Local(comp.actor_ref()));
            comps.push(comp);
        }

        let mut channel_strategy: Box<ChannelStrategy<Input, TestComp>> =
            Box::new(KeyBy::with_default_hasher(parallelism, channels));

        let mut rng = rand::thread_rng();

        let mut inputs: Vec<ArconEvent<Input>> = Vec::new();
        for _i in 0..total_msgs {
            let input = Input {
                id: rng.gen_range(0, 100),
            };
            inputs.push(ArconEvent::Element(ArconElement::new(input)));
        }

        for input in inputs {
            // Just assume it is all sent from same comp
            let comp_def = &*comps.get(0 as usize).unwrap().definition().lock().unwrap();
            let _ = channel_strategy.output(input, comp_def);
        }

        std::thread::sleep(std::time::Duration::from_secs(1));

        // Each of the 8 components should at least get some hits
        for comp in comps {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert!(comp_inspect.counter > 0);
        }
        let _ = system.shutdown();
    }
}
