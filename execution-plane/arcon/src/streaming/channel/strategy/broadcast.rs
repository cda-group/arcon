use crate::data::{ArconMessage, ArconType};
use crate::prelude::KompactSystem;
use crate::streaming::channel::strategy::{channel_output, ChannelStrategy};
use crate::streaming::channel::Channel;
use arcon_error::ArconResult;

pub struct Broadcast<A>
where
    A: 'static + ArconType,
{
    out_channels: Vec<Channel<A>>,
}

impl<A> Broadcast<A>
where
    A: 'static + ArconType,
{
    pub fn new(out_channels: Vec<Channel<A>>) -> Broadcast<A> {
        Broadcast { out_channels }
    }
}

impl<A> ChannelStrategy<A> for Broadcast<A>
where
    A: 'static + ArconType,
{
    fn output(&mut self, message: ArconMessage<A>, source: &KompactSystem) -> ArconResult<()> {
        for channel in &self.out_channels {
            channel_output(channel, message.clone(), source)?;
        }
        Ok(())
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
    use crate::streaming::channel::ArconSerde;
    use kompact::prelude::*;
    use std::sync::Arc;

    #[test]
    fn broadcast_local_test() {
        let system = KompactConfig::default().build().expect("KompactSystem");

        let components: u32 = 8;
        let total_msgs: u64 = 10;

        let mut channels: Vec<Channel<Input>> = Vec::new();
        let mut comps: Vec<Arc<crate::prelude::Component<DebugNode<Input>>>> = Vec::new();

        for _i in 0..components {
            let comp = system.create_and_start(move || DebugNode::<Input>::new());
            let actor_ref: ActorRefStrong<ArconMessage<Input>> =
                comp.actor_ref().hold().expect("failed to fetch");
            channels.push(Channel::Local(actor_ref));
            comps.push(comp);
        }

        let mut channel_strategy: Box<dyn ChannelStrategy<Input>> =
            Box::new(Broadcast::new(channels));

        for _i in 0..total_msgs {
            let input = ArconMessage::element(Input { id: 1 }, None, 1.into());
            // Just assume it is all sent from same comp
            let _ = channel_strategy.output(input, &system);
        }

        std::thread::sleep(std::time::Duration::from_secs(1));

        // Each of the 8 components should havesame amount of msgs..
        for comp in comps {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.data.len() as u64, total_msgs);
        }
        let _ = system.shutdown();
    }

    #[test]
    fn broadcast_local_and_remote() {
        let (system, remote) = {
            let system = || {
                let mut cfg = KompactConfig::new();
                cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
                cfg.build().expect("KompactSystem")
            };
            (system(), system())
        };

        let local_components: u32 = 4;
        let remote_components: u32 = 4;
        let total_msgs: u64 = 5;

        let mut channels: Vec<Channel<Input>> = Vec::new();
        let mut comps: Vec<Arc<crate::prelude::Component<DebugNode<Input>>>> = Vec::new();

        // Create local components
        for _i in 0..local_components {
            let comp = system.create_and_start(move || DebugNode::<Input>::new());
            let actor_ref: ActorRefStrong<ArconMessage<Input>> =
                comp.actor_ref().hold().expect("failed to fetch");
            channels.push(Channel::Local(actor_ref));
            comps.push(comp);
        }

        // Create remote components
        for i in 0..remote_components {
            let comp = remote.create_and_start(move || DebugNode::<Input>::new());
            let comp_id = format!("comp_{}", i);
            let _ = remote.register_by_alias(&comp, comp_id.clone());
            remote.start(&comp);

            let remote_path = ActorPath::Named(NamedPath::with_system(
                remote.system_path(),
                vec![comp_id.into()],
            ));
            channels.push(Channel::Remote((remote_path, ArconSerde::default())));
            comps.push(comp);
        }
        std::thread::sleep(std::time::Duration::from_secs(1));

        let mut channel_strategy: Box<dyn ChannelStrategy<Input>> =
            Box::new(Broadcast::new(channels));

        for _i in 0..total_msgs {
            let input = ArconMessage::element(Input { id: 1 }, None, 1.into());
            // Just assume it is all sent from same comp
            let _ = channel_strategy.output(input, &system);
        }

        std::thread::sleep(std::time::Duration::from_secs(1));

        // Each of the 8 components should have same amount of msgs..
        for comp in comps {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.data.len() as u64, total_msgs);
        }
        let _ = system.shutdown();
    }
}
