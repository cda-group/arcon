use crate::data::{ArconEvent, ArconType};
use crate::error::*;
use crate::streaming::channel::strategy::*;
use crate::streaming::channel::Channel;
use std::marker::PhantomData;

pub struct RoundRobin<A>
where
    A: 'static + ArconType,
{
    out_channels: Vec<Channel>,
    curr_index: usize,
    phantom_a: PhantomData<A>,
}

impl<A> RoundRobin<A>
where
    A: 'static + ArconType,
{
    pub fn new(out_channels: Vec<Channel>) -> RoundRobin<A> {
        RoundRobin {
            out_channels,
            curr_index: 0,
            phantom_a: PhantomData,
        }
    }
}

impl<A> ChannelStrategy<A> for RoundRobin<A>
where
    A: 'static + ArconType,
{
    fn output(&mut self, event: ArconEvent<A>, source: &KompactSystem) -> ArconResult<()> {
        match event {
            ArconEvent::Element(_) => {
                if self.out_channels.is_empty() {
                    return arcon_err!("{}", "Vector of Channels is empty");
                }

                if let Some(channel) = self.out_channels.get(self.curr_index) {
                    channel_output(channel, event, source)?;
                    self.curr_index += 1;

                    if self.curr_index >= self.out_channels.len() {
                        self.curr_index = 0;
                    }

                    Ok(())
                } else {
                    arcon_err!("Unable to select channel")
                }
            }
            _ => {
                for channel in &self.out_channels {
                    channel_output(channel, event, source)?;
                }
                Ok(())
            }
        }
    }

    fn add_channel(&mut self, channel: Channel) {
        self.out_channels.push(channel);
    }
    fn remove_channel(&mut self, _channel: Channel) {
        unimplemented!();
    }
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

        let mut channel_strategy: Box<ChannelStrategy<Input>> = Box::new(RoundRobin::new(channels));

        for _i in 0..total_msgs {
            let input = ArconEvent::Element(ArconElement::new(Input { id: 1 }));
            // Just assume it is all sent from same comp
            let _ = channel_strategy.output(input, &system);
        }

        std::thread::sleep(std::time::Duration::from_secs(1));

        for comp in comps {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.counter, total_msgs / components);
        }
        let _ = system.shutdown();
    }
}
