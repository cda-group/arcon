use crate::streaming::partitioner::Partitioner;
use crate::streaming::Channel;
use std::hash::Hash;
use std::marker::PhantomData;

pub struct Broadcast<A: 'static + Send + Sync + Copy + Hash> {
    out_channels: Vec<Channel>,
    phantom: PhantomData<A>,
}

impl<A: 'static + Send + Sync + Copy + Hash> Broadcast<A> {
    pub fn new(out_channels: Vec<Channel>) -> Broadcast<A> {
        Broadcast {
            out_channels,
            phantom: PhantomData,
        }
    }
}

impl<A: 'static + Send + Sync + Copy + Hash> Partitioner<A> for Broadcast<A> {
    fn output(&mut self, event: A, source: &Channel, key: Option<u64>) -> crate::error::Result<()> {
        for channel in &self.out_channels {
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
        Ok(())
    }

    fn add_channel(&mut self, channel: Channel) {
        self.out_channels.push(channel);
    }
    fn remove_channel(&mut self, channel: Channel) {
        self.out_channels.retain(|c| c == &channel);
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
    fn broadcast_test() {
        let mut cfg = KompactConfig::new();
        cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let components: u32 = 8;
        let total_msgs = 10;

        let mut channels: Vec<Channel> = Vec::new();
        let mut comps: Vec<Arc<crate::prelude::Component<TestComp>>> = Vec::new();

        for i in 0..components {
            let comp = system.create_and_start(move || TestComp::new());
            channels.push(Channel::Local(comp.actor_ref()));
            comps.push(comp);
        }

        let mut partitioner: Box<Partitioner<Input>> =
            Box::new(Broadcast::new(channels.clone()));

        for i in 0..total_msgs {
            // NOTE: second parameter is a fake channel...
            let input = Input {id: 1};
            let _ = partitioner.output(input, &channels.get(0).unwrap().clone(), None);
        }

        std::thread::sleep(std::time::Duration::from_secs(1));

        // Each of the 8 components shouldhave same amount of msgs..
        for comp in comps {
            let mut comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.counter, total_msgs);
        }
        system.shutdown();
    }
}
