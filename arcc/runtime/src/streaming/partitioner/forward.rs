use crate::streaming::partitioner::Partitioner;
use crate::streaming::Channel;
use std::hash::Hash;
use std::marker::PhantomData;

pub struct Forward<A: 'static + Send + Sync + Copy + Hash> {
    out_channel: Channel,
    phantom: PhantomData<A>,
}
impl<A: 'static + Send + Sync + Copy + Hash> Forward<A> {
    pub fn new(out_channel: Channel) -> Forward<A> {
        Forward {
            out_channel,
            phantom: PhantomData,
        }
    }
}

impl<A: 'static + Send + Sync + Copy + Hash> Partitioner<A> for Forward<A> {
    fn output(&mut self, event: A, source: &Channel, key: Option<u64>) -> crate::error::Result<()> {
        match &self.out_channel {
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
        Ok(())
    }
    fn add_channel(&mut self, channel: Channel) {
        // ignore
    }
    fn remove_channel(&mut self, channel: Channel) {
        // ignore
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
    #[derive(Clone, Copy, Hash)]
    pub struct Input {
        id: u32,
    }

    #[test]
    fn forward_test() {
        let mut cfg = KompactConfig::new();
        cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let components: u32 = 8;
        let total_msgs = 10;
        let comp = system.create_and_start(move || TestComp::new());
        let channel = Channel::Local(comp.actor_ref());

        let mut partitioner: Box<Partitioner<Input>> =
            Box::new(Forward::new(channel.clone()));

        for i in 0..total_msgs {
            // NOTE: second parameter is a fake channel...
            let input = Input {id: 1};
            let _ = partitioner.output(input, &channel.clone(), None);
        }

        std::thread::sleep(std::time::Duration::from_secs(1));
        let mut comp_inspect = &comp.definition().lock().unwrap();
        assert_eq!(comp_inspect.counter, total_msgs);
        system.shutdown();
    }
}
