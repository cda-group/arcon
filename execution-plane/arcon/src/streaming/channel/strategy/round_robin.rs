use crate::data::{ArconEvent, ArconType};
use crate::error::*;
use crate::streaming::channel::strategy::*;
use crate::streaming::channel::Channel;
use kompact::{ComponentDefinition, Port, Require};

pub struct RoundRobin<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconEvent<A>> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
    out_channels: Vec<Channel<A, B, C>>,
    curr_index: usize,
}

impl<A, B, C> RoundRobin<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconEvent<A>> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
    pub fn new(out_channels: Vec<Channel<A, B, C>>) -> RoundRobin<A, B, C> {
        RoundRobin {
            out_channels,
            curr_index: 0,
        }
    }
}

impl<A, B, C> ChannelStrategy<A, B, C> for RoundRobin<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconEvent<A>> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
    fn output(&mut self, element: ArconEvent<A>, source: *const C) -> ArconResult<()> {
        if self.out_channels.is_empty() {
            return arcon_err!("{}", "Vector of Channels is empty");
        }

        let channel = &(*self.out_channels.get(self.curr_index).unwrap());
        let _ = channel_output(channel, element, source)?;
        self.curr_index += 1;

        if self.curr_index >= self.out_channels.len() {
            self.curr_index = 0;
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

unsafe impl<A, B, C> Send for RoundRobin<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconEvent<A>> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
}

unsafe impl<A, B, C> Sync for RoundRobin<A, B, C>
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
    use crate::streaming::channel::strategy::tests::*;
    use crate::streaming::channel::{ChannelPort, RequirePortRef};
    use kompact::*;
    use std::cell::UnsafeCell;
    use std::rc::Rc;
    use std::sync::Arc;

    #[test]
    fn round_robin_local_test() {
        let cfg = KompactConfig::new();
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let components: u64 = 8;
        let total_msgs: u64 = components * 4;

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

        let mut channel_strategy: Box<ChannelStrategy<Input, ChannelPort<Input>, TestComp>> =
            Box::new(RoundRobin::new(channels));

        for _i in 0..total_msgs {
            let input = ArconEvent::Element(ArconElement::new(Input { id: 1 }));
            // Just assume it is all sent from same comp
            let comp_def = &(*comps.get(0 as usize).unwrap().definition().lock().unwrap());
            let _ = channel_strategy.output(input, comp_def);
        }

        std::thread::sleep(std::time::Duration::from_secs(1));

        for comp in comps {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.counter, total_msgs / components);
        }
        let _ = system.shutdown();
    }
}
