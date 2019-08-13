use crate::data::{ArconEvent, ArconType};
use crate::error::*;
use crate::streaming::channel::strategy::{channel_output, ChannelStrategy};
use crate::streaming::channel::Channel;
use kompact::ComponentDefinition;
use std::marker::PhantomData;

pub struct Forward<A, B>
where
    A: 'static + ArconType,
    B: ComponentDefinition + Sized + 'static,
{
    out_channel: Channel,
    phantom_a: PhantomData<A>,
    phantom_b: PhantomData<B>,
}

impl<A, B> Forward<A, B>
where
    A: 'static + ArconType,
    B: ComponentDefinition + Sized + 'static,
{
    pub fn new(out_channel: Channel) -> Forward<A, B> {
        Forward {
            out_channel,
            phantom_a: PhantomData,
            phantom_b: PhantomData,
        }
    }
}

impl<A, B> ChannelStrategy<A, B> for Forward<A, B>
where
    A: 'static + ArconType,
    B: ComponentDefinition + Sized + 'static,
{
    fn output(&mut self, event: ArconEvent<A>, source: *const B) -> ArconResult<()> {
        channel_output(&self.out_channel, event, source)
    }
    fn add_channel(&mut self, _channel: Channel) {
        // ignore
    }
    fn remove_channel(&mut self, _channel: Channel) {
        // ignore
    }
}

unsafe impl<A, B> Send for Forward<A, B>
where
    A: 'static + ArconType,
    B: ComponentDefinition + Sized + 'static,
{
}

unsafe impl<A, B> Sync for Forward<A, B>
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

    #[test]
    fn forward_test() {
        let cfg = KompactConfig::new();
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let total_msgs = 10;
        let comp = system.create_and_start(move || TestComp::new());
        let mut channel_strategy: Box<ChannelStrategy<Input, TestComp>> =
            Box::new(Forward::new(Channel::Local(comp.actor_ref())));

        for _i in 0..total_msgs {
            // NOTE: second parameter is a fake channel...
            let input = ArconEvent::Element(ArconElement::new(Input { id: 1 }));
            let _ = channel_strategy.output(input, &*comp.definition().lock().unwrap());
        }

        std::thread::sleep(std::time::Duration::from_secs(1));
        let comp_inspect = &comp.definition().lock().unwrap();
        assert_eq!(comp_inspect.counter, total_msgs);
        let _ = system.shutdown();
    }
}
