use crate::data::{ArconMessage, ArconType};
use crate::error::*;
use crate::streaming::channel::strategy::*;
use crate::streaming::channel::Channel;

pub struct RoundRobin<A>
where
    A: 'static + ArconType,
{
    out_channels: Vec<Channel<A>>,
    curr_index: usize,
}

impl<A> RoundRobin<A>
where
    A: 'static + ArconType,
{
    pub fn new(out_channels: Vec<Channel<A>>) -> RoundRobin<A> {
        RoundRobin {
            out_channels,
            curr_index: 0,
        }
    }
}

impl<A> ChannelStrategy<A> for RoundRobin<A>
where
    A: 'static + ArconType,
{
    fn output(&mut self, message: ArconMessage<A>, source: &KompactSystem) -> ArconResult<()> {
        match message.event {
            ArconEvent::Element(_) => {
                if self.out_channels.is_empty() {
                    return arcon_err!("{}", "Vector of Channels is empty");
                }

                if let Some(channel) = self.out_channels.get(self.curr_index) {
                    channel_output(channel, message, source)?;
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
        unimplemented!();
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
    fn round_robin_local_test() {
        let system = KompactConfig::default().build().expect("KompactSystem");

        let components: u64 = 8;
        let total_msgs: u64 = components * 4;

        let mut channels: Vec<Channel<Input>> = Vec::new();
        let mut comps: Vec<Arc<crate::prelude::Component<DebugNode<Input>>>> = Vec::new();

        // Create half of the channels using ActorRefs
        for _i in 0..components {
            let comp = system.create_and_start(move || DebugNode::<Input>::new());
            let actor_ref: ActorRefStrong<ArconMessage<Input>> =
                comp.actor_ref().hold().expect("failed to fetch");
            channels.push(Channel::Local(actor_ref));
            comps.push(comp);
        }

        let mut channel_strategy: Box<dyn ChannelStrategy<Input>> =
            Box::new(RoundRobin::new(channels));

        for _i in 0..total_msgs {
            let input = ArconMessage::element(Input { id: 1 }, None, 1.into());
            // Just assume it is all sent from same comp
            let _ = channel_strategy.output(input, &system);
        }

        std::thread::sleep(std::time::Duration::from_secs(1));

        for comp in comps {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.data.len() as u64, total_msgs / components);
        }
        let _ = system.shutdown();
    }
}
