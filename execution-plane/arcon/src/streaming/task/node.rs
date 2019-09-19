use crate::prelude::*;
/*
    Node: contains Task which executes actions
*/
#[derive(ComponentDefinition)]
pub struct Node<IN, OUT>
where
    IN: 'static + ArconType,
    OUT: 'static + ArconType,
{
    ctx: ComponentContext<Node<IN, OUT>>,
    id: String,
    out_channels: Box<ChannelStrategy<OUT>>,
    in_channels: Vec<String>,
    task: Box<Task<IN, OUT>>,
}

impl<IN, OUT> Node<IN, OUT>
where
    IN: 'static + ArconType,
    OUT: 'static + ArconType,
{
    pub fn new(
        id: String,
        in_channels: Vec<String>,
        out_channels: Box<ChannelStrategy<OUT>>,
        task: Box<Task<IN, OUT>>,
    ) -> Node<IN, OUT> {
        Node {
            ctx: ComponentContext::new(),
            id,
            out_channels,
            in_channels,
            task,
        }
    }
    fn handle_message(&mut self, message: &ArconMessage<IN>) -> ArconResult<()> {
        if !self.in_channels.contains(&message.sender) {
            return arcon_err!("Message from invalid sender");
        }
        match message.event {
            ArconEvent::Element(e) => {
                let results = self.task.handle_element(e)?;
                for result in results {
                    self.output_event(result)?;
                }
            }
            ArconEvent::Watermark(w) => {
                let results = self.task.handle_watermark(w)?;
                for result in results {
                    self.output_event(result)?;
                }
                self.output_event(ArconEvent::<OUT>::Watermark(w))?; // Forward the watermark
            }
        }
        Ok(())
    }
    fn output_event(&mut self, event: ArconEvent<OUT>) -> ArconResult<()> {
        let message = ArconMessage {
            event: event,
            sender: self.id.clone(),
        };
        self.out_channels.output(message, &self.ctx.system())
    }
}

impl<IN, OUT> Provide<ControlPort> for Node<IN, OUT>
where
    IN: 'static + ArconType,
    OUT: 'static + ArconType,
{
    fn handle(&mut self, event: ControlEvent) -> () {
        match event {
            ControlEvent::Start => {}
            _ => {
                error!(self.ctx.log(), "bad ControlEvent");
            }
        }
    }
}

impl<IN, OUT> Actor for Node<IN, OUT>
where
    IN: 'static + ArconType,
    OUT: 'static + ArconType,
{
    type Message = ArconMessage<IN>;

    fn receive_local(&mut self, msg: Self::Message) {
        if let Err(err) = self.handle_message(&msg) {
            error!(self.ctx.log(), "Failed to handle message: {}", err);
        }
    }
    fn receive_network(&mut self, _msg: NetMessage) {
        unimplemented!();
        /*
        match msg.try_deserialise::<ArconMessage<IN>, ProtoSer>() {
            Ok(node_msg) => {
                if let Err(err) = self.handle_message(&node_msg) {
                    error!(self.ctx.log(), "Failed to handle node message: {}", err);
                }
            }
            Err(e) => error!(self.ctx.log(), "Error deserialising ArconMessage: {:?}", e),
        }
        */
    }
}

unsafe impl<IN, OUT> Send for Node<IN, OUT>
where
    IN: 'static + ArconType,
    OUT: 'static + ArconType,
{
}

unsafe impl<IN, OUT> Sync for Node<IN, OUT>
where
    IN: 'static + ArconType,
    OUT: 'static + ArconType,
{
}

#[cfg(test)]
mod tests {
    // Tested implicitly in integration tests of the tasks.
}
