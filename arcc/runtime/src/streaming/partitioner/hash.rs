use crate::data::{ArconElement, ArconType};
use crate::error::*;
use crate::prelude::{DeserializeOwned, Serialize};
use crate::streaming::partitioner::channel_output;
use crate::streaming::partitioner::Partitioner;
use crate::streaming::Channel;
use kompact::{ComponentDefinition, Port, Require};
use messages::protobuf::*;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::default::Default;
use std::fmt::Debug;
use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};

/// A hash based partitioner
///
/// `HashPartitioner` may be constructed with
/// either a custom hasher or the default std one
pub struct HashPartitioner<A, B, C, D = BuildHasherDefault<DefaultHasher>>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<A>> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
    builder: D,
    parallelism: u32,
    map: HashMap<usize, Channel<A, B, C>, D>,
}

impl<A, B, C> HashPartitioner<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<A>> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
    pub fn with_hasher<H: BuildHasher + Default>(
        builder: H,
        parallelism: u32,
        channels: Vec<Channel<A, B, C>>,
    ) -> HashPartitioner<A, B, C, H> {
        assert_eq!(channels.len(), parallelism as usize);
        let mut map = HashMap::with_capacity_and_hasher(parallelism as usize, Default::default());
        for (i, channel) in channels.into_iter().enumerate() {
            map.insert(i, channel);
        }
        HashPartitioner {
            builder: builder.into(),
            parallelism,
            map,
        }
    }

    pub fn with_default_hasher<H>(
        parallelism: u32,
        channels: Vec<Channel<A, B, C>>,
    ) -> HashPartitioner<A, B, C, BuildHasherDefault<H>>
    where
        H: Hasher + Default,
    {
        assert_eq!(channels.len(), parallelism as usize);
        let mut map = HashMap::with_capacity_and_hasher(parallelism as usize, Default::default());
        for (i, channel) in channels.into_iter().enumerate() {
            map.insert(i, channel);
        }
        HashPartitioner {
            builder: BuildHasherDefault::<H>::default(),
            parallelism,
            map,
        }
    }
}

impl<A, B, C> Partitioner<A, B, C> for HashPartitioner<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<A>> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
    fn output(
        &mut self,
        element: ArconElement<A>,
        source: *const C,
        key: Option<u64>,
    ) -> ArconResult<()> {
        let mut h = self.builder.build_hasher();
        element.data.hash(&mut h);
        let hash = h.finish() as u32;
        let id = (hash % self.parallelism) as i32;
        if id >= 0 && id <= self.parallelism as i32 {
            if let Some(channel) = self.map.get(&(id as usize)) {
                let _ = channel_output(channel, element, source, key)?;
            }
        } else {
            // TODO: Fix
            panic!("Failed to hash to channel properly..");
        }
        Ok(())
    }

    fn add_channel(&mut self, channel: Channel<A, B, C>) {
        unimplemented!();
    }
    fn remove_channel(&mut self, channel: Channel<A, B, C>) {
        unimplemented!();
    }
}

unsafe impl<A, B, C> Send for HashPartitioner<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<A>> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
}

unsafe impl<A, B, C> Sync for HashPartitioner<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<A>> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::Serialize;
    use crate::streaming::partitioner::tests::*;
    use crate::streaming::{ChannelPort, RequirePortRef};
    use kompact::default_components::*;
    use kompact::*;
    use rand::Rng;
    use std::sync::Arc;

    #[test]
    fn partitioner_parallelism_8_test() {
        let mut cfg = KompactConfig::new();
        cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let parallelism: u32 = 8;
        let total_msgs = 1000;

        let mut channels: Vec<Channel<Input, ChannelPort<Input>, TestComp>> = Vec::new();
        let mut comps: Vec<Arc<crate::prelude::Component<TestComp>>> = Vec::new();

        for i in 0..parallelism {
            let comp = system.create_and_start(move || TestComp::new());
            channels.push(Channel::Local(comp.actor_ref()));
            comps.push(comp);
        }

        let mut partitioner: Box<Partitioner<Input, ChannelPort<Input>, TestComp>> =
            Box::new(HashPartitioner::with_default_hasher(parallelism, channels));

        let mut rng = rand::thread_rng();

        let mut inputs: Vec<ArconElement<Input>> = Vec::new();
        for i in 0..total_msgs {
            let input = Input {
                id: rng.gen_range(0, 100),
            };
            inputs.push(ArconElement::new(input));
        }

        for input in inputs {
            // Just assume it is all sent from same comp
            let comp_def = &*comps.get(0 as usize).unwrap().definition().lock().unwrap();
            let _ = partitioner.output(input, comp_def, None);
        }

        std::thread::sleep(std::time::Duration::from_secs(1));

        // Each of the 8 components should at least get some hits
        for comp in comps {
            let mut comp_inspect = &comp.definition().lock().unwrap();
            assert!(comp_inspect.counter > 0);
        }
        system.shutdown();
    }
}
