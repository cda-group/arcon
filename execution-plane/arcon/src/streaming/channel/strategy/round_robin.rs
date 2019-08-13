use crate::data::{ArconEvent, ArconType};
use crate::error::*;
use crate::streaming::channel::strategy::*;
use crate::streaming::channel::Channel;
use kompact::ComponentDefinition;
use std::marker::PhantomData;

pub struct RoundRobin<A, B>
where
    A: 'static + ArconType,
    B: ComponentDefinition + Sized + 'static,
{
    out_channels: Vec<Channel>,
    curr_index: usize,
    phantom_a: PhantomData<A>,
    phantom_b: PhantomData<B>,
}

impl<A, B> RoundRobin<A, B>
where
    A: 'static + ArconType,
    B: ComponentDefinition + Sized + 'static,
{
    pub fn new(out_channels: Vec<Channel>) -> RoundRobin<A, B> {
        RoundRobin {
            out_channels,
            curr_index: 0,
            phantom_a: PhantomData,
            phantom_b: PhantomData,
        }
    }
}

impl<A, B> ChannelStrategy<A, B> for RoundRobin<A, B>
where
    A: 'static + ArconType,
    B: ComponentDefinition + Sized + 'static,
{
    fn output(&mut self, element: ArconEvent<A>, source: *const B) -> ArconResult<()> {
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

    fn add_channel(&mut self, channel: Channel) {
        self.out_channels.push(channel);
    }
    fn remove_channel(&mut self, _channel: Channel) {
        unimplemented!();
    }
}

unsafe impl<A, B> Send for RoundRobin<A, B>
where
    A: 'static + ArconType,
    B: ComponentDefinition + Sized + 'static,
{
}

unsafe impl<A, B> Sync for RoundRobin<A, B>
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
    use kompact::*;
    use std::sync::Arc;

    #[test]
    fn round_robin_local_test() {
        let cfg = KompactConfig::new();
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let components: u64 = 8;
        let total_msgs: u64 = components * 4;

        let mut channels: Vec<Channel> = Vec::new();
        let mut comps: Vec<Arc<crate::prelude::Component<TestComp>>> = Vec::new();

        // Create half of the channels using ActorRefs
        for _i in 0..components {
            let comp = system.create_and_start(move || TestComp::new());
            channels.push(Channel::Local(comp.actor_ref()));
            comps.push(comp);
        }

        let mut channel_strategy: Box<ChannelStrategy<Input, TestComp>> =
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
