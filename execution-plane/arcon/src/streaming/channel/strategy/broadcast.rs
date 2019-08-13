use crate::data::{ArconEvent, ArconType};
use crate::error::*;
use crate::streaming::channel::strategy::{channel_output, ChannelStrategy};
use crate::streaming::channel::Channel;
use kompact::ComponentDefinition;
use std::marker::PhantomData;

pub struct Broadcast<A, B>
where
    A: 'static + ArconType,
    B: ComponentDefinition + Sized + 'static,
{
    out_channels: Vec<Channel>,
    phantom_a: PhantomData<A>,
    phantom_b: PhantomData<B>,
}

impl<A, B> Broadcast<A, B>
where
    A: 'static + ArconType,
    B: ComponentDefinition + Sized + 'static,
{
    pub fn new(out_channels: Vec<Channel>) -> Broadcast<A, B> {
        Broadcast {
            out_channels,
            phantom_a: PhantomData,
            phantom_b: PhantomData,
        }
    }
}

impl<A, B> ChannelStrategy<A, B> for Broadcast<A, B>
where
    A: 'static + ArconType,
    B: ComponentDefinition + Sized + 'static,
{
    fn output(&mut self, event: ArconEvent<A>, source: *const B) -> ArconResult<()> {
        for channel in &self.out_channels {
            let _ = channel_output(channel, event, source)?;
        }
        Ok(())
    }

    fn add_channel(&mut self, channel: Channel) {
        self.out_channels.push(channel);
    }
    fn remove_channel(&mut self, _channel: Channel) {
        unimplemented!();
    }
}

unsafe impl<A, B> Send for Broadcast<A, B>
where
    A: 'static + ArconType,
    B: ComponentDefinition + Sized + 'static,
{
}

unsafe impl<A, B> Sync for Broadcast<A, B>
where
    A: 'static + ArconType,
    B: ComponentDefinition + Sized + 'static,
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::ArconElement;
    use crate::streaming::channel::strategy::tests::*;
    use kompact::default_components::*;
    use kompact::*;
    use std::sync::Arc;

    #[test]
    fn broadcast_local_test() {
        let cfg = KompactConfig::new();
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let components: u32 = 8;
        let total_msgs: u64 = 10;

        let mut channels: Vec<Channel> = Vec::new();
        let mut comps: Vec<Arc<crate::prelude::Component<TestComp>>> = Vec::new();

        for _i in 0..components {
            let comp = system.create_and_start(move || TestComp::new());
            channels.push(Channel::Local(comp.actor_ref()));
            comps.push(comp);
        }

        let mut channel_strategy: Box<ChannelStrategy<Input, TestComp>> =
            Box::new(Broadcast::new(channels));

        for _i in 0..total_msgs {
            let input = ArconEvent::Element(ArconElement::new(Input { id: 1 }));
            // Just assume it is all sent from same comp
            let comp_def = &(*comps.get(0 as usize).unwrap().definition().lock().unwrap());
            let _ = channel_strategy.output(input, comp_def);
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

        let mut channels: Vec<Channel> = Vec::new();
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

        let mut channel_strategy: Box<ChannelStrategy<Input, TestComp>> =
            Box::new(Broadcast::new(channels));

        for _i in 0..total_msgs {
            let input = ArconElement::new(Input { id: 1 });
            // Just assume it is all sent from same comp
            let comp_def = &(*comps.get(0 as usize).unwrap().definition().lock().unwrap());
            let _ = channel_strategy.output(ArconEvent::Element(input), comp_def);
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
