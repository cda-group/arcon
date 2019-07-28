use crate::prelude::*;

#[derive(ComponentDefinition)]
pub struct DebugSink<A>
where
    A: ArconType + 'static,
{
    ctx: ComponentContext<Self>,
    pub in_port: ProvidedPort<ChannelPort<A>, Self>,
}

impl<A> DebugSink<A>
where
    A: ArconType + 'static,
{
    pub fn new() -> Self {
        DebugSink {
            ctx: ComponentContext::new(),
            in_port: ProvidedPort::new(),
        }
    }
}

impl<A> Provide<ControlPort> for DebugSink<A>
where
    A: ArconType + 'static,
{
    fn handle(&mut self, _event: ControlEvent) -> () {}
}

impl<A> Actor for DebugSink<A>
where
    A: ArconType + 'static,
{
    fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
        if let Some(element) = msg.downcast_ref::<ArconElement<A>>() {
            info!(self.ctx.log(), "Sink element: {:?}", element.data);
        }
    }
    fn receive_message(&mut self, _sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {}
}

impl<A> Provide<ChannelPort<A>> for DebugSink<A>
where
    A: ArconType + 'static,
{
    fn handle(&mut self, element: ArconElement<A>) -> () {
        info!(self.ctx.log(), "Sink element: {:?}", element.data);
    }
}

impl<A> Require<ChannelPort<A>> for DebugSink<A>
where
    A: ArconType + 'static,
{
    fn handle(&mut self, _event: ()) -> () {}
}
