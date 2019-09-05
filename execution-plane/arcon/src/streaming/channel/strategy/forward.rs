
use crate::prelude::*;
use crate::streaming::channel::strategy::{channel_output, ChannelStrategy};
use crate::streaming::channel::Channel;
use kompact::KompactSystem;
use std::marker::PhantomData;

pub struct Forward<A>
where
    A: 'static + ArconType,
{
    out_channel: Channel,
    phantom_a: PhantomData<A>,
}

impl<A> Forward<A>
where
    A: 'static + ArconType,
{
    pub fn new(out_channel: Channel) -> Forward<A> {
        Forward {
            out_channel,
            phantom_a: PhantomData,
        }
    }
}

impl<A> ChannelStrategy<A> for Forward<A>
where
    A: 'static + ArconType,
{
    fn output(&mut self, message: ArconMessage<A>, source: &KompactSystem) -> ArconResult<()> {
        channel_output(&self.out_channel, message, source)
    }
    fn add_channel(&mut self, _channel: Channel) {
        // ignore
    }
    fn remove_channel(&mut self, _channel: Channel) {
        // ignore
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::channel::strategy::tests::*;
    use kompact::*;

    #[test]
    fn forward_test() {
        let cfg = KompactConfig::new();
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let total_msgs = 10;
        let comp = system.create_and_start(move || DebugSink::<Input>::new());
        let mut channel_strategy: Box<ChannelStrategy<Input>> =
            Box::new(Forward::new(Channel::Local(comp.actor_ref())));

        for _i in 0..total_msgs {
            // NOTE: second parameter is a fake channel...
            let input = ArconMessage::element(Input{id:1}, None, "test".to_string());
            let _ = channel_strategy.output(input, &system);
        }

        std::thread::sleep(std::time::Duration::from_secs(1));
        let comp_inspect = &comp.definition().lock().unwrap();
        assert_eq!(comp_inspect.data.len(), total_msgs);
        let _ = system.shutdown();
    }
}
