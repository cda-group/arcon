use crate::prelude::*;

#[derive(ComponentDefinition)]
pub struct DebugNode<IN>
where
    IN: ArconType,
{
    ctx: ComponentContext<DebugNode<IN>>,
    pub data: Vec<ArconElement<IN>>,
    pub watermarks: Vec<Watermark>,
    pub epochs: Vec<Epoch>,
}

impl<IN> DebugNode<IN>
where
    IN: ArconType,
{
    pub fn new() -> DebugNode<IN> {
        DebugNode {
            ctx: ComponentContext::new(),
            data: Vec::new(),
            watermarks: Vec::new(),
            epochs: Vec::new(),
        }
    }
    fn handle_event(&mut self, event: ArconEvent<IN>) {
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

impl<IN> Default for DebugNode<IN>
where
    IN: ArconType,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<IN> Provide<ControlPort> for DebugNode<IN>
where
    IN: ArconType,
{
    fn handle(&mut self, event: ControlEvent) {
        match event {
            ControlEvent::Start => {
                debug!(self.ctx.log(), "Started Arcon DebugNode");
            }
            ControlEvent::Stop => {
                // TODO
            }
            ControlEvent::Kill => {
                // TODO
            }
        }
    }
}

impl<IN> Actor for DebugNode<IN>
where
    IN: ArconType,
{
    type Message = ArconMessage<IN>;

    fn receive_local(&mut self, msg: Self::Message) {
        self.handle_event(msg.event);
    }
    fn receive_network(&mut self, msg: NetMessage) {
        let arcon_msg: ArconResult<ArconMessage<IN>> = match *msg.ser_id() {
            ReliableSerde::<IN>::SER_ID => msg
                .try_deserialise::<ArconMessage<IN>, ReliableSerde<IN>>()
                .map_err(|_| arcon_err_kind!("Failed to unpack reliable ArconMessage")),
            UnsafeSerde::<IN>::SER_ID => msg
                .try_deserialise::<ArconMessage<IN>, UnsafeSerde<IN>>()
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

unsafe impl<IN> Send for DebugNode<IN> where IN: ArconType {}

unsafe impl<IN> Sync for DebugNode<IN> where IN: ArconType {}
