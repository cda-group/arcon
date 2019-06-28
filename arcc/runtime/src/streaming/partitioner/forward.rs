use crate::error::ErrorKind::*;
use crate::error::*;
use crate::prelude::Serialize;
use crate::streaming::partitioner::channel_output;
use crate::streaming::partitioner::Partitioner;
use crate::streaming::Channel;
use kompact::ComponentDefinition;
use messages::protobuf::*;
use std::hash::Hash;
use std::marker::PhantomData;

pub struct Forward<A, B>
where
    A: 'static + Serialize + Send + Sync + Copy + Hash,
    B: ComponentDefinition + Sized + 'static,
{
    out_channel: Channel,
    phantom_event: PhantomData<A>,
    phantom_source: PhantomData<B>,
}

impl<A, B> Forward<A, B>
where
    A: 'static + Serialize + Send + Sync + Copy + Hash,
    B: ComponentDefinition + Sized + 'static,
{
    pub fn new(out_channel: Channel) -> Forward<A, B> {
        Forward {
            out_channel,
            phantom_event: PhantomData,
            phantom_source: PhantomData,
        }
    }
}

impl<A, B> Partitioner<A, B> for Forward<A, B>
where
    A: 'static + Serialize + Send + Sync + Copy + Hash,
    B: ComponentDefinition + Sized + 'static,
{
    fn output(&mut self, event: A, source: *const B, key: Option<u64>) -> crate::error::Result<()> {
        channel_output(&self.out_channel, event, source, key)
    }
    fn add_channel(&mut self, channel: Channel) {
        // ignore
    }
    fn remove_channel(&mut self, channel: Channel) {
        // ignore
    }
}

unsafe impl<A, B> Send for Forward<A, B>
where
    A: 'static + Serialize + Send + Sync + Copy + Hash,
    B: ComponentDefinition + Sized + 'static,
{
}

unsafe impl<A, B> Sync for Forward<A, B>
where
    A: 'static + Serialize + Send + Sync + Copy + Hash,
    B: ComponentDefinition + Sized + 'static,
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::Serialize;
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
    #[derive(Clone, Copy, Hash, Serialize)]
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

        let mut partitioner: Box<Partitioner<Input, TestComp>> =
            Box::new(Forward::new(channel.clone()));

        for i in 0..total_msgs {
            // NOTE: second parameter is a fake channel...
            let input = Input { id: 1 };
            let _ = partitioner.output(input, &*comp.definition().lock().unwrap(), None);
        }

        std::thread::sleep(std::time::Duration::from_secs(1));
        let mut comp_inspect = &comp.definition().lock().unwrap();
        assert_eq!(comp_inspect.counter, total_msgs);
        system.shutdown();
    }
}
