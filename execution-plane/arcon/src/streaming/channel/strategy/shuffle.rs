use crate::data::{ArconEvent, ArconType};
use crate::error::*;
use crate::streaming::channel::strategy::*;
use crate::streaming::channel::Channel;
use rand::seq::SliceRandom;

pub struct Shuffle<A>
where
    A: 'static + ArconType,
{
    out_channels: Vec<Channel<A>>,
}

impl<A> Shuffle<A>
where
    A: 'static + ArconType,
{
    pub fn new(out_channels: Vec<Channel<A>>) -> Shuffle<A> {
        Shuffle { out_channels }
    }
}

impl<A> ChannelStrategy<A> for Shuffle<A>
where
    A: 'static + ArconType,
{
    fn output(&mut self, message: ArconMessage<A>, source: &KompactSystem) -> ArconResult<()> {
        match message.event {
            ArconEvent::Element(_) => {
                if let Some(channel) = self.out_channels.choose(&mut rand::thread_rng()) {
                    channel_output(channel, message, source)?;
                } else {
                    return arcon_err!("{}", "Failed to pick Channel while shuffling");
                }
                Ok(())
            }
            _ => {
                for channel in &self.out_channels {
                    channel_output(channel, message.clone(), source)?;
                }
                Ok(())
            }
        }
    }

    fn add_channel(&mut self, channel: Channel<A>) {
        self.out_channels.push(channel);
    }
    fn remove_channel(&mut self, _channel: Channel<A>) {
        //self.out_channels.retain(|c| c == &channel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::DebugNode;
    use crate::streaming::channel::strategy::tests::*;
    use kompact::prelude::*;
    use std::sync::Arc;

    #[test]
    fn shuffle_local_test() {
        let system = KompactConfig::default().build().expect("KompactSystem");

        let components: u32 = 4;
        let total_msgs: u64 = 50;

        let mut channels: Vec<Channel<Input>> = Vec::new();
        let mut comps: Vec<Arc<crate::prelude::Component<DebugNode<Input>>>> = Vec::new();

        for _i in 0..components {
            let comp = system.create_and_start(move || DebugNode::<Input>::new());
            let actor_ref: ActorRefStrong<ArconMessage<Input>> =
                comp.actor_ref().hold().expect("failed to fetch");
            channels.push(Channel::Local(actor_ref));
            comps.push(comp);
        }

        let mut channel_strategy: Box<ChannelStrategy<Input>> = Box::new(Shuffle::new(channels));

        for _i in 0..total_msgs {
            let input = ArconMessage::element(Input { id: 1 }, None, 1.into());
            // Just assume it is all sent from same comp
            let _ = channel_strategy.output(input, &system);
        }

        std::thread::sleep(std::time::Duration::from_secs(1));

        // All components should be hit
        for comp in comps {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert!(comp_inspect.data.len() > 0);
        }
        let _ = system.shutdown();
    }
}
