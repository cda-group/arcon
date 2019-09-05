use crate::data::ArconEvent;
use crate::prelude::*;
use crate::messages::protobuf::ProtoSer;

#[derive(ComponentDefinition)]
pub struct DebugSink<A>
where
    A: ArconType + 'static,
{
    ctx: ComponentContext<Self>,
    pub data: Vec<ArconElement<A>>,
    pub watermarks: Vec<Watermark>,
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
        }
    }

    fn handle_event(&mut self, event: &ArconEvent<A>) {
        match event {
            ArconEvent::Element(e) => {
                info!(self.ctx.log(), "Sink element: {:?}", e.data);
                self.data.push(*e);
            }
            ArconEvent::Watermark(w) => {
                self.watermarks.push(*w);
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
    fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
        if let Some(message) = msg.downcast_ref::<ArconMessage<A>>() {
            self.handle_event(&message.event);
        }
    }
    fn receive_message(&mut self, sender: ActorPath, ser_id: u64, buf: &mut Buf) {
        if ser_id == serialisation_ids::PBUF {
            let r = ProtoSer::deserialise(buf);
            if let Ok(msg) = r {
                if let Ok(message) = ArconMessage::from_remote(msg) {
                    self.handle_event(&message.event);
                } else {
                    error!(self.ctx.log(), "Failed to convert remote message to local");
                }
            } else {
                error!(self.ctx.log(), "Failed to deserialise StreamTaskMessage",);
            }
        } else {
            error!(self.ctx.log(), "Got unexpected message from {}", sender);
        }
    }
}
