use crate::streaming::partitioner::Partitioner;
use crate::streaming::Channel;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::default::Default;
use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};
use std::marker::PhantomData;

/// A hash based partitioner
///
/// `HashPartitioner` may be constructed with
/// either a custom hasher or the default std one
pub struct HashPartitioner<
    A: 'static + Send + Sync + Copy + Hash,
    H = BuildHasherDefault<DefaultHasher>,
> {
    builder: H,
    parallelism: u32,
    map: HashMap<usize, Channel, H>,
    phantom: PhantomData<A>,
}

impl<A: 'static + Send + Sync + Copy + Hash> HashPartitioner<A> {
    pub fn with_hasher<B: BuildHasher + Default>(
        builder: B,
        parallelism: u32,
        channels: Vec<Channel>,
    ) -> HashPartitioner<A, B> {
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
            phantom: PhantomData,
        }
    }

    pub fn with_default_hasher<B>(
        parallelism: u32,
        channels: Vec<Channel>,
    ) -> HashPartitioner<A, BuildHasherDefault<B>>
    where
        B: Hasher + Default,
    {
        assert_eq!(channels.len(), parallelism as usize);
        let mut map = HashMap::with_capacity_and_hasher(parallelism as usize, Default::default());
        for i in 0..channels.len() as usize {
            let channel: Channel = channels.get(i).take().unwrap().clone();
            map.insert(i, channel);
        }
        HashPartitioner {
            builder: BuildHasherDefault::<B>::default(),
            parallelism,
            map,
            phantom: PhantomData,
        }
    }
}

impl<A: 'static + Send + Sync + Copy + Hash> Partitioner<A> for HashPartitioner<A> {
    fn output(&mut self, event: A, source: &Channel, key: Option<u64>) -> crate::error::Result<()> {
        let mut h = self.builder.build_hasher();
        event.hash(&mut h);
        let hash = h.finish() as u32;
        let id = (hash % self.parallelism) as i32;
        if id >= 0 && id <= self.parallelism as i32 {
            if let Some(channel) = self.map.get(&(id as usize)) {
                match channel {
                    Channel::Local(actor_ref) => {
                        // TODO: extract source and insert as second arg
                        actor_ref.tell(Box::new(event), actor_ref);
                    }
                    Channel::Remote(actor_path) => {
                        if let Some(key) = key {
                            // KeyedElement
                            unimplemented!();
                        } else {
                            // Element
                            unimplemented!();
                        }
                    }
                }
            }
        } else {
            // ERR
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
    #[derive(Clone, Copy)]
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

        let mut partitioner: Box<Partitioner<Input>> = Box::new(
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
            // NOTE: second parameter is a fake channel...
            let _ = partitioner.output(input, &channels.get(0).unwrap().clone(), None);
        }

        std::thread::sleep(std::time::Duration::from_secs(1));

        // Each of the 8 components should at least get some hits
        for comp in comps {
            let mut comp_inspect = &comp.definition().lock().unwrap();
            assert!(comp_inspect.counter > 0);
        }
        system.shutdown();
    }

    // TODO: Create tests with both ActorRefs and ActorPaths
}
