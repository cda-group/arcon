use crate::data::{ArconElement, ArconType};
use crate::error::*;
use crate::streaming::channel::strategy::{channel_output, ChannelStrategy};
use crate::streaming::channel::Channel;
use kompact::{ComponentDefinition, Port, Require};

pub struct Forward<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<A>> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
    out_channel: Channel<A, B, C>,
}

impl<A, B, C> Forward<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<A>> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
    pub fn new(out_channel: Channel<A, B, C>) -> Forward<A, B, C> {
        Forward { out_channel }
    }
}

impl<A, B, C> ChannelStrategy<A, B, C> for Forward<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<A>> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
    fn output(
        &mut self,
        event: ArconElement<A>,
        source: *const C,
        key: Option<u64>,
    ) -> ArconResult<()> {
        channel_output(&self.out_channel, event, source, key)
    }
    fn add_channel(&mut self, _channel: Channel<A, B, C>) {
        // ignore
    }
    fn remove_channel(&mut self, _channel: Channel<A, B, C>) {
        // ignore
    }
}

unsafe impl<A, B, C> Send for Forward<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<A>> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
}

unsafe impl<A, B, C> Sync for Forward<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<A>> + 'static + Clone,
    C: ComponentDefinition + Sized + 'static + Require<B>,
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::channel::strategy::tests::*;
    use crate::streaming::channel::{ChannelPort, RequirePortRef};
    use kompact::*;
    use std::cell::UnsafeCell;
    use std::rc::Rc;

    #[test]
    fn forward_test() {
        let cfg = KompactConfig::new();
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let total_msgs = 10;
        let comp = system.create_and_start(move || TestComp::new());
        let target_port = comp.on_definition(|c| c.prov_port.share());

        let mut req_port: RequiredPort<ChannelPort<Input>, TestComp> = RequiredPort::new();
        let _ = req_port.connect(target_port);

        let ref_port = RequirePortRef(Rc::new(UnsafeCell::new(req_port)));
        let comp_channel: Channel<Input, ChannelPort<Input>, TestComp> = Channel::Port(ref_port);

        let mut channel_strategy: Box<ChannelStrategy<Input, ChannelPort<Input>, TestComp>> =
            Box::new(Forward::new(comp_channel));

        for _i in 0..total_msgs {
            // NOTE: second parameter is a fake channel...
            let input = ArconElement::new(Input { id: 1 });
            let _ = channel_strategy.output(input, &*comp.definition().lock().unwrap(), None);
        }

        std::thread::sleep(std::time::Duration::from_secs(1));
        let comp_inspect = &comp.definition().lock().unwrap();
        assert_eq!(comp_inspect.counter, total_msgs);
        let _ = system.shutdown();
    }
}
