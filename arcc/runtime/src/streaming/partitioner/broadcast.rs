use crate::error::*;
use crate::prelude::{DeserializeOwned, Serialize};
use crate::streaming::partitioner::channel_output;
use crate::streaming::partitioner::Partitioner;
use crate::streaming::Channel;
use kompact::{ComponentDefinition, Port, Require};
use messages::protobuf::*;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;

pub struct Broadcast<A, B, C>
where
    A: 'static + Serialize + DeserializeOwned + Send + Sync + Copy + Hash + Debug,
    B: Port<Request = A> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
    out_channels: Vec<Channel<A, B, C>>,
}

impl<A, B, C> Broadcast<A, B, C>
where
    A: 'static + Serialize + DeserializeOwned + Send + Sync + Copy + Hash + Debug,
    B: Port<Request = A> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
    pub fn new(out_channels: Vec<Channel<A, B, C>>) -> Broadcast<A, B, C> {
        Broadcast { out_channels }
    }
}

impl<A, B, C> Partitioner<A, B, C> for Broadcast<A, B, C>
where
    A: 'static + Serialize + DeserializeOwned + Send + Sync + Copy + Hash + Debug,
    B: Port<Request = A> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
    fn output(&mut self, event: A, source: *const C, key: Option<u64>) -> ArconResult<()> {
        for channel in &self.out_channels {
            let _ = channel_output(channel, event, source, key)?;
        }
        Ok(())
    }

    fn add_channel(&mut self, channel: Channel<A, B, C>) {
        self.out_channels.push(channel);
    }
    fn remove_channel(&mut self, channel: Channel<A, B, C>) {
        //self.out_channels.retain(|c| c == &channel);
    }
}

unsafe impl<A, B, C> Send for Broadcast<A, B, C>
where
    A: 'static + Serialize + DeserializeOwned + Send + Sync + Copy + Hash + Debug,
    B: Port<Request = A> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
}

unsafe impl<A, B, C> Sync for Broadcast<A, B, C>
where
    A: 'static + Serialize + DeserializeOwned + Send + Sync + Copy + Hash + Debug,
    B: Port<Request = A> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::partitioner::tests::*;
    use crate::streaming::{ChannelPort, RequirePortRef};
    use kompact::default_components::*;
    use kompact::*;
    use rand::Rng;
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;

    #[test]
    fn broadcast_local_test() {
        let mut cfg = KompactConfig::new();
        cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let components: u32 = 8;
        let total_msgs: u64 = 10;

        let mut channels: Vec<Channel<Input, ChannelPort<Input>, TestComp>> = Vec::new();
        let mut comps: Vec<Arc<crate::prelude::Component<TestComp>>> = Vec::new();

        // Create half of the channels using ActorRefs
        for i in 0..components / 2 {
            let comp = system.create_and_start(move || TestComp::new());
            channels.push(Channel::Local(comp.actor_ref()));
            comps.push(comp);
        }

        // Create half of the channels using Ports
        for i in 0..components / 2 {
            let comp = system.create_and_start(move || TestComp::new());
            let target_port = comp.on_definition(|c| c.prov_port.share());
            let mut req_port: RequiredPort<ChannelPort<Input>, TestComp> = RequiredPort::new();
            let _ = req_port.connect(target_port);

            let ref_port = RequirePortRef(Rc::new(RefCell::new(req_port)));
            let comp_channel: Channel<Input, ChannelPort<Input>, TestComp> =
                Channel::Port(ref_port);
            channels.push(comp_channel);
            comps.push(comp);
        }
        let mut partitioner: Box<Partitioner<Input, ChannelPort<Input>, TestComp>> =
            Box::new(Broadcast::new(channels));

        for i in 0..total_msgs {
            let input = Input { id: 1 };
            // Just assume it is all sent from same comp
            let comp_def = &(*comps.get(0 as usize).unwrap().definition().lock().unwrap());
            let _ = partitioner.output(input, comp_def, None);
        }

        std::thread::sleep(std::time::Duration::from_secs(1));

        // Each of the 8 components should havesame amount of msgs..
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

        let mut channels: Vec<Channel<Input, ChannelPort<Input>, TestComp>> = Vec::new();
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
        std::thread::sleep(std::time::Duration::from_secs(1));

        let mut partitioner: Box<Partitioner<Input, ChannelPort<Input>, TestComp>> =
            Box::new(Broadcast::new(channels));

        for i in 0..total_msgs {
            let input = Input { id: 1 };
            // Just assume it is all sent from same comp
            let comp_def = &(*comps.get(0 as usize).unwrap().definition().lock().unwrap());
            let _ = partitioner.output(input, comp_def, None);
        }

        std::thread::sleep(std::time::Duration::from_secs(1));

        // Each of the 8 components should have same amount of msgs..
        for comp in comps {
            let mut comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.counter, total_msgs);
        }
        system.shutdown();
    }
}
