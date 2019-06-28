use crate::error::ErrorKind::*;
use crate::error::*;
use crate::prelude::Serialize;
use crate::streaming::partitioner::Partitioner;
use crate::streaming::Channel;
use kompact::ComponentDefinition;
use messages::protobuf::*;
use std::hash::Hash;
use std::marker::PhantomData;

pub struct Broadcast<A, B>
where
    A: 'static + Serialize + Send + Sync + Copy + Hash,
    B: ComponentDefinition + Sized + 'static,
{
    out_channels: Vec<Channel>,
    phantom_event: PhantomData<A>,
    phantom_source: PhantomData<B>,
}

impl<A, B> Broadcast<A, B>
where
    A: 'static + Serialize + Send + Sync + Copy + Hash,
    B: ComponentDefinition + Sized + 'static,
{
    pub fn new(out_channels: Vec<Channel>) -> Broadcast<A, B> {
        Broadcast {
            out_channels,
            phantom_event: PhantomData,
            phantom_source: PhantomData,
        }
    }
}

impl<A, B> Partitioner<A, B> for Broadcast<A, B>
where
    A: 'static + Serialize + Send + Sync + Copy + Hash,
    B: ComponentDefinition + Sized + 'static,
{
    fn output(&mut self, event: A, source: &B, key: Option<u64>) -> crate::error::Result<()> {
        for channel in &self.out_channels {
            match channel {
                Channel::Local(actor_ref) => {
                    actor_ref.tell(Box::new(event), source);
                }
                Channel::Remote(actor_path) => {
                    let serialised_event: Vec<u8> = bincode::serialize(&event)
                        .map_err(|e| Error::new(SerializationError(e.to_string())))?;

                    if let Some(key) = key {
                        let keyed_msg = create_keyed_element(serialised_event, 1, key);
                        actor_path.tell(keyed_msg, source);
                    } else {
                        let element_msg = create_element(serialised_event, 1);
                        actor_path.tell(element_msg, source);
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
        fn receive_message(&mut self, sender: ActorPath, ser_id: u64, buf: &mut Buf) {
            self.counter += 1;
        }
    }

    #[repr(C)]
    #[derive(Clone, Copy, Hash, Serialize)]
    pub struct Input {
        id: u32,
    }

    #[test]
    fn broadcast_local_test() {
        let mut cfg = KompactConfig::new();
        cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let components: u32 = 8;
        let total_msgs: u64 = 10;

        let mut channels: Vec<Channel> = Vec::new();
        let mut comps: Vec<Arc<crate::prelude::Component<TestComp>>> = Vec::new();

        for i in 0..components {
            let comp = system.create_and_start(move || TestComp::new());
            channels.push(Channel::Local(comp.actor_ref()));
            comps.push(comp);
        }

        let mut partitioner: Box<Partitioner<Input, TestComp>> =
            Box::new(Broadcast::new(channels.clone()));

        for i in 0..total_msgs {
            let input = Input { id: 1 };
            // Just assume it is all sent from same comp
            let comp_def = &comps.get(0 as usize).unwrap().definition().lock().unwrap();
            let _ = partitioner.output(input, comp_def, None);
        }

        std::thread::sleep(std::time::Duration::from_secs(1));

        // Each of the 8 components shouldhave same amount of msgs..
        for comp in comps {
            let mut comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.counter, total_msgs);
        }
        system.shutdown();
    }

    #[test]
    fn broadcast_local_and_remote() {
        let (system, remote) = {
            let system = || {
                let mut cfg = KompactConfig::new();
                cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
                KompactSystem::new(cfg).expect("KompactSystem")
            };
            (system(), system())
        };

        let local_components: u32 = 4;
        let remote_components: u32 = 4;
        let total_msgs: u64 = 5;

        let mut channels: Vec<Channel> = Vec::new();
        let mut comps: Vec<Arc<crate::prelude::Component<TestComp>>> = Vec::new();

        // Create local components
        for i in 0..local_components {
            let comp = system.create_and_start(move || TestComp::new());
            channels.push(Channel::Local(comp.actor_ref()));
            comps.push(comp);
        }

        // Create remote components
        for i in 0..remote_components {
            let comp = remote.create_and_start(move || TestComp::new());
            let comp_id = format!("comp_{}", i);
            let _ = remote.register_by_alias(&comp, comp_id.clone());
            remote.start(&comp);

            let remote_path = ActorPath::Named(NamedPath::with_system(
                remote.system_path(),
                vec![comp_id.into()],
            ));
            channels.push(Channel::Remote(remote_path));
            comps.push(comp);
        }

        let mut partitioner: Box<Partitioner<Input, TestComp>> =
            Box::new(Broadcast::new(channels.clone()));

        for i in 0..total_msgs {
            let input = Input { id: 1 };
            // Just assume it is all sent from same comp
            let comp_def = &comps.get(0 as usize).unwrap().definition().lock().unwrap();
            let _ = partitioner.output(input, comp_def, None);
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
