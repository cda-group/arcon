use crate::data::{ArconEvent, ArconType};
use crate::error::*;
use crate::streaming::partitioner::channel_output;
use crate::streaming::partitioner::Partitioner;
use crate::streaming::Channel;
use kompact::{ComponentDefinition, Port, Require};

pub struct Broadcast<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconEvent<A>> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
    out_channels: Vec<Channel<A, B, C>>,
}

impl<A, B, C> Broadcast<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconEvent<A>> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
    pub fn new(out_channels: Vec<Channel<A, B, C>>) -> Broadcast<A, B, C> {
        Broadcast { out_channels }
    }
}

impl<A, B, C> Partitioner<A, B, C> for Broadcast<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconEvent<A>> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
    fn output(&mut self, event: ArconEvent<A>, source: *const C) -> ArconResult<()> {
        for channel in &self.out_channels {
            let _ = channel_output(channel, event, source)?;
        }
        Ok(())
    }

    fn add_channel(&mut self, channel: Channel<A, B, C>) {
        self.out_channels.push(channel);
    }
    fn remove_channel(&mut self, _channel: Channel<A, B, C>) {
        unimplemented!();
    }
}

unsafe impl<A, B, C> Send for Broadcast<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconEvent<A>> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
}

unsafe impl<A, B, C> Sync for Broadcast<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconEvent<A>> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::ArconElement;
    use crate::streaming::partitioner::tests::*;
    use crate::streaming::{ChannelPort, RequirePortRef};
    use kompact::default_components::*;
    use kompact::*;
    use std::cell::UnsafeCell;
    use std::rc::Rc;
    use std::sync::Arc;

    #[test]
    fn broadcast_local_test() {
        let cfg = KompactConfig::new();
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let components: u32 = 8;
        let total_msgs: u64 = 10;

        let mut channels: Vec<Channel<Input, ChannelPort<Input>, TestComp>> = Vec::new();
        let mut comps: Vec<Arc<crate::prelude::Component<TestComp>>> = Vec::new();

        // Create half of the channels using ActorRefs
        for _i in 0..components / 2 {
            let comp = system.create_and_start(move || TestComp::new());
            channels.push(Channel::Local(comp.actor_ref()));
            comps.push(comp);
        }

        // Create half of the channels using Ports
        for _i in 0..components / 2 {
            let comp = system.create_and_start(move || TestComp::new());
            let target_port = comp.on_definition(|c| c.prov_port.share());
            let mut req_port: RequiredPort<ChannelPort<Input>, TestComp> = RequiredPort::new();
            let _ = req_port.connect(target_port);

            let ref_port = RequirePortRef(Rc::new(UnsafeCell::new(req_port)));
            let comp_channel: Channel<Input, ChannelPort<Input>, TestComp> =
                Channel::Port(ref_port);
            channels.push(comp_channel);
            comps.push(comp);
        }
        let mut partitioner: Box<Partitioner<Input, ChannelPort<Input>, TestComp>> =
            Box::new(Broadcast::new(channels));

        for _i in 0..total_msgs {
            let input = ArconEvent::Element(ArconElement::new(Input { id: 1 }));
            // Just assume it is all sent from same comp
            let comp_def = &(*comps.get(0 as usize).unwrap().definition().lock().unwrap());
            let _ = partitioner.output(input, comp_def);
        }

        std::thread::sleep(std::time::Duration::from_secs(1));

        // Each of the 8 components should havesame amount of msgs..
        for comp in comps {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.counter, total_msgs);
        }
        let _ = system.shutdown();
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
        for _i in 0..local_components {
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

        for _i in 0..total_msgs {
            let input = ArconElement::new(Input { id: 1 });
            // Just assume it is all sent from same comp
            let comp_def = &(*comps.get(0 as usize).unwrap().definition().lock().unwrap());
            let _ = partitioner.output(ArconEvent::Element(input), comp_def);
        }

        std::thread::sleep(std::time::Duration::from_secs(1));

        // Each of the 8 components should have same amount of msgs..
        for comp in comps {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.counter, total_msgs);
        }
        let _ = system.shutdown();
    }
}
