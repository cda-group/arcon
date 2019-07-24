use crate::data::{ArconEvent, ArconType};
use crate::error::*;
use crate::streaming::partitioner::channel_output;
use crate::streaming::partitioner::Partitioner;
use crate::streaming::Channel;
use kompact::{ComponentDefinition, Port, Require};
use rand::seq::SliceRandom;

pub struct Shuffle<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconEvent<A>> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
    out_channels: Vec<Channel<A, B, C>>,
}

impl<A, B, C> Shuffle<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconEvent<A>> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
    pub fn new(out_channels: Vec<Channel<A, B, C>>) -> Shuffle<A, B, C> {
        Shuffle { out_channels }
    }
}

impl<A, B, C> Partitioner<A, B, C> for Shuffle<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconEvent<A>> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
    fn output(&mut self, element: ArconEvent<A>, source: *const C) -> ArconResult<()> {
        if let Some(channel) = self.out_channels.choose(&mut rand::thread_rng()) {
            let _ = channel_output(channel, element, source)?;
        } else {
            return arcon_err!("{}", "Failed to pick Channel while shuffling");
        }

        Ok(())
    }

    fn add_channel(&mut self, channel: Channel<A, B, C>) {
        self.out_channels.push(channel);
    }
    fn remove_channel(&mut self, _channel: Channel<A, B, C>) {
        //self.out_channels.retain(|c| c == &channel);
    }
}

unsafe impl<A, B, C> Send for Shuffle<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconEvent<A>> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
}

unsafe impl<A, B, C> Sync for Shuffle<A, B, C>
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
    use crate::streaming::ChannelPort;
    use kompact::*;
    use std::sync::Arc;

    #[test]
    fn shuffle_local_test() {
        let cfg = KompactConfig::new();
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let components: u32 = 4;
        let total_msgs: u64 = 50;

        let mut channels: Vec<Channel<Input, ChannelPort<Input>, TestComp>> = Vec::new();
        let mut comps: Vec<Arc<crate::prelude::Component<TestComp>>> = Vec::new();

        // Create half of the channels using ActorRefs
        for _i in 0..components {
            let comp = system.create_and_start(move || TestComp::new());
            channels.push(Channel::Local(comp.actor_ref()));
            comps.push(comp);
        }

        let mut partitioner: Box<Partitioner<Input, ChannelPort<Input>, TestComp>> =
            Box::new(Shuffle::new(channels));

        for _i in 0..total_msgs {
            let input = ArconEvent::Element(ArconElement::new(Input { id: 1 }));
            // Just assume it is all sent from same comp
            let comp_def = &(*comps.get(0 as usize).unwrap().definition().lock().unwrap());
            let _ = partitioner.output(input, comp_def);
        }

        std::thread::sleep(std::time::Duration::from_secs(1));

        // All components should be hit
        for comp in comps {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert!(comp_inspect.counter > 0);
        }
        let _ = system.shutdown();
    }
}
