use crate::streaming::task::partitioner::Partitioner;
use crate::streaming::task::partitioner::Task;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::default::Default;
use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};
use std::marker::PhantomData;

/// A hash based partitioner for Streaming Tasks
///
/// `HashPartitioner` may be constructed with
/// either a custom hasher or the default std one
pub struct HashPartitioner<H = BuildHasherDefault<DefaultHasher>> {
    builder: H,
    parallelism: u32,
    map: HashMap<usize, Task, H>,
}

impl HashPartitioner {
    pub fn with_hasher<B: BuildHasher + Default>(
        builder: B,
        parallelism: u32,
        tasks: Vec<Task>,
    ) -> HashPartitioner<B> {
        assert_eq!(tasks.len(), parallelism as usize);
        let mut map = HashMap::with_capacity_and_hasher(parallelism as usize, Default::default());
        for i in 0..tasks.len() as usize {
            let task: Task = tasks.get(i).take().unwrap().clone();
            map.insert(i, task);
        }
        HashPartitioner {
            builder: builder.into(),
            parallelism,
            map,
        }
    }

    pub fn with_default_hasher<B>(
        parallelism: u32,
        tasks: Vec<Task>,
    ) -> HashPartitioner<BuildHasherDefault<B>>
    where
        B: Hasher + Default,
    {
        assert_eq!(tasks.len(), parallelism as usize);
        let mut map = HashMap::with_capacity_and_hasher(parallelism as usize, Default::default());
        for i in 0..tasks.len() as usize {
            let task: Task = tasks.get(i).take().unwrap().clone();
            map.insert(i, task);
        }
        HashPartitioner {
            builder: BuildHasherDefault::<B>::default(),
            parallelism,
            map,
        }
    }
}

impl<A: Hash> Partitioner<A> for HashPartitioner {
    fn get_task(&mut self, input: A) -> Option<Task> {
        let mut h = self.builder.build_hasher();
        input.hash(&mut h);
        let hash = h.finish() as u32;
        let id = (hash % self.parallelism) as i32;
        if id >= 0 && id <= self.parallelism as i32 {
            Some(self.map.get(&(id as usize)).unwrap().clone())
        } else {
            None
        }
    }
    fn add_task(&mut self, task: Task) {
        unimplemented!();
    }

    fn remove_task(&mut self, task: Task) {
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
        pub id: u64,
        pub counter: u64,
    }

    impl TestComp {
        pub fn new(id: u64) -> TestComp {
            TestComp {
                ctx: ComponentContext::new(),
                id,
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

        let mut tasks: Vec<Task> = Vec::new();
        let mut comps: Vec<Arc<crate::prelude::Component<TestComp>>> = Vec::new();

        for i in 0..parallelism {
            let (comp, _) = system.create_and_register(move || TestComp::new(i as u64));
            system.start(&comp);
            tasks.push(Task::Local(comp.actor_ref()));
            comps.push(comp);
        }

        let mut partitioner: Box<Partitioner<Input>> =
            Box::new(HashPartitioner::with_default_hasher(parallelism, tasks));

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
            match partitioner.get_task(input) {
                Some(Task::Local(actor_ref)) => {
                    actor_ref.tell(Box::new(input), &actor_ref);
                }
                Some(Task::Remote(actor_path)) => {}
                None => {}
            }
        }

        std::thread::sleep(std::time::Duration::from_secs(1));

        // Each of the 8 components should at least get some hits
        for comp in comps {
            let mut comp_inspect = &comp.definition().lock().unwrap();
            assert!(comp_inspect.counter > 0);
        }
    }

    // TODO: Create tests with both ActorRefs and ActorPaths
}
