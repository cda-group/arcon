use crate::data::ArconEvent;
use crate::prelude::*;

#[derive(ComponentDefinition)]
pub struct DebugSink<A>
where
    A: ArconType + 'static,
{
    ctx: ComponentContext<Self>,
    pub data: Vec<ArconElement<A>>,
    pub watermarks: Vec<Watermark>,
    pub epochs: Vec<Epoch>,
}

impl<A> DebugSink<A>
where
    A: ArconType + 'static,
{
    pub fn new() -> Self {
        DebugSink {
            ctx: ComponentContext::new(),
            data: Vec::new(),
            watermarks: Vec::new(),
            epochs: Vec::new(),
        }
    }

    fn handle_event(&mut self, event: ArconEvent<A>) {
        match event {
            ArconEvent::Element(e) => {
                info!(self.ctx.log(), "Sink element: {:?}", e.data);
                self.data.push(e);
            }
            ArconEvent::Watermark(w) => {
                self.watermarks.push(w);
            }
            ArconEvent::Epoch(e) => {
                self.epochs.push(e);
            }
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
    type Message = ArconMessage<A>;

    fn receive_local(&mut self, msg: Self::Message) {
        self.handle_event(msg.event);
    }
    fn receive_network(&mut self, msg: NetMessage) {
        let arcon_msg: ArconResult<ArconMessage<A>> = match msg.ser_id() {
            &ReliableSerde::<A>::SER_ID => msg
                .try_deserialise::<ArconMessage<A>, ReliableSerde<A>>()
                .map_err(|_| arcon_err_kind!("Failed to unpack reliable ArconMessage")),
            &UnsafeSerde::<A>::SER_ID => msg
                .try_deserialise::<ArconMessage<A>, UnsafeSerde<A>>()
                .map_err(|_| arcon_err_kind!("Failed to unpack unreliable ArconMessage")),
            _ => panic!("Unexpected deserialiser"),
        };

        match arcon_msg {
            Ok(m) => {
                self.handle_event(m.event);
            }
            Err(e) => error!(self.ctx.log(), "Error ArconNetworkMessage: {:?}", e),
        }
    }
}
