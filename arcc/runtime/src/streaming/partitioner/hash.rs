use crate::error::ErrorKind::*;
use crate::error::*;
use crate::prelude::Serialize;
use crate::streaming::partitioner::Partitioner;
use crate::streaming::Channel;
use kompact::ComponentDefinition;
use messages::protobuf::*;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::default::Default;
use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};
use std::marker::PhantomData;
use crate::streaming::partitioner::channel_output;

/// A hash based partitioner
///
/// `HashPartitioner` may be constructed with
/// either a custom hasher or the default std one
pub struct HashPartitioner<A, B, C = BuildHasherDefault<DefaultHasher>>
where
    A: 'static + Serialize + Send + Sync + Copy + Hash,
    B: ComponentDefinition + Sized + 'static,
{
    builder: C,
    parallelism: u32,
    map: HashMap<usize, Channel, C>,
    phantom_event: PhantomData<A>,
    phantom_source: PhantomData<B>,
}

impl<A, B> HashPartitioner<A, B>
where
    A: 'static + Serialize + Send + Sync + Copy + Hash,
    B: ComponentDefinition + Sized + 'static,
{
    pub fn with_hasher<H: BuildHasher + Default>(
        builder: H,
        parallelism: u32,
        channels: Vec<Channel>,
    ) -> HashPartitioner<A, B, H> {
        assert_eq!(channels.len(), parallelism as usize);
        let mut map = HashMap::with_capacity_and_hasher(parallelism as usize, Default::default());
        for i in 0..channels.len() as usize {
            let channel: Channel = channels.get(i).take().unwrap().clone();
            map.insert(i, channel);
        }
        HashPartitioner {
            builder: builder.into(),
            parallelism,
            map,
            phantom_event: PhantomData,
            phantom_source: PhantomData,
        }
    }

    pub fn with_default_hasher<H>(
        parallelism: u32,
        channels: Vec<Channel>,
    ) -> HashPartitioner<A, B, BuildHasherDefault<H>>
    where
        H: Hasher + Default,
    {
        assert_eq!(channels.len(), parallelism as usize);
        let mut map = HashMap::with_capacity_and_hasher(parallelism as usize, Default::default());
        for i in 0..channels.len() as usize {
            let channel: Channel = channels.get(i).take().unwrap().clone();
            map.insert(i, channel);
        }
        HashPartitioner {
            builder: BuildHasherDefault::<H>::default(),
            parallelism,
            map,
            phantom_event: PhantomData,
            phantom_source: PhantomData,
        }
    }
}

impl<A, B> Partitioner<A, B> for HashPartitioner<A, B>
where
    A: 'static + Serialize + Send + Sync + Copy + Hash,
    B: ComponentDefinition + Sized + 'static,
{
    fn output(&mut self, event: A, source: &B, key: Option<u64>) -> crate::error::Result<()> {
        let mut h = self.builder.build_hasher();
        event.hash(&mut h);
        let hash = h.finish() as u32;
        let id = (hash % self.parallelism) as i32;
        if id >= 0 && id <= self.parallelism as i32 {
            if let Some(channel) = self.map.get(&(id as usize)) {
                let _ = channel_output(channel, event, source, key)?;
            }
        } else {
            // TODO: Fix
            panic!("Failed to hash to channel properly..");
        }
        Ok(())
    }

    fn add_channel(&mut self, channel: Channel) {
        unimplemented!();
    }
    fn remove_channel(&mut self, channel: Channel) {
        unimplemented!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kompact::default_components::*;
    use kompact::*;
    use rand::Rng;
    use std::sync::Arc;

    #[derive(ComponentDefinition)]
    #[allow(dead_code)]
    pub struct TestComp {
        ctx: ComponentContext<TestComp>,
        pub counter: u64,
    }

    impl TestComp {
        pub fn new() -> TestComp {
            TestComp {
                ctx: ComponentContext::new(),
                counter: 0,
            }
        }
    }
    impl Provide<ControlPort> for TestComp {
        fn handle(&mut self, event: ControlEvent) -> () {}
    }

    impl Actor for TestComp {
        fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
            self.counter += 1;
        }
        fn receive_message(&mut self, sender: ActorPath, ser_id: u64, buf: &mut Buf) {}
    }

    #[repr(C)]
    #[key_by(id)]
    #[derive(Clone, Copy, Serialize)]
    pub struct Input {
        id: u32,
        price: u64,
    }

    #[test]
    fn partitioner_parallelism_8_test() {
        let mut cfg = KompactConfig::new();
        cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let parallelism: u32 = 8;
        let total_msgs = 1000;

        let mut channels: Vec<Channel> = Vec::new();
        let mut comps: Vec<Arc<crate::prelude::Component<TestComp>>> = Vec::new();

        for i in 0..parallelism {
            let comp = system.create_and_start(move || TestComp::new());
            channels.push(Channel::Local(comp.actor_ref()));
            comps.push(comp);
        }

        let mut partitioner: Box<Partitioner<Input, TestComp>> = Box::new(
            HashPartitioner::with_default_hasher(parallelism, channels.clone()),
        );

        let mut rng = rand::thread_rng();

        let mut inputs: Vec<Input> = Vec::new();
        for i in 0..total_msgs {
            let input = Input {
                id: rng.gen_range(0, 100),
                price: 30,
            };
            inputs.push(input);
        }

        for input in inputs {
            // Just assume it is all sent from same comp
            let comp_def = &comps.get(0 as usize).unwrap().definition().lock().unwrap();
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
