use crate::prelude::*;
use crate::streaming::channel::strategy::{channel_output, ChannelStrategy};
use crate::streaming::channel::Channel;

pub struct Forward<A>
where
    A: 'static + ArconType,
{
    out_channel: Channel<A>,
}

impl<A> Forward<A>
where
    A: 'static + ArconType,
{
    pub fn new(out_channel: Channel<A>) -> Forward<A> {
        Forward { out_channel }
    }
}

impl<A> ChannelStrategy<A> for Forward<A>
where
    A: 'static + ArconType,
{
    fn output(&mut self, message: ArconMessage<A>, source: &KompactSystem) -> ArconResult<()> {
        channel_output(&self.out_channel, message, source)
    }
    fn add_channel(&mut self, _channel: Channel<A>) {
        // ignore
    }
    fn remove_channel(&mut self, _channel: Channel<A>) {
        // ignore
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::channel::strategy::tests::*;
    use kompact::prelude::*;

    #[test]
    fn forward_test() {
        let system = KompactConfig::default().build().expect("KompactSystem");

        let total_msgs = 10;
        let comp = system.create_and_start(move || DebugSink::<Input>::new());
        let actor_ref: ActorRefStrong<ArconMessage<Input>> =
            comp.actor_ref().hold().expect("failed to fetch");
        let mut channel_strategy: Box<ChannelStrategy<Input>> =
            Box::new(Forward::new(Channel::Local(actor_ref)));

        for _i in 0..total_msgs {
            let input = ArconMessage::element(Input { id: 1 }, None, 1.into());
            let _ = channel_strategy.output(input, &system);
        }

        std::thread::sleep(std::time::Duration::from_secs(1));
        {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.data.len(), total_msgs);
        }
        let _ = system.shutdown();
    }
}
