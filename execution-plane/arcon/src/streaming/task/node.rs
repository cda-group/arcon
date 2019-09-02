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
    out_channels: Box<ChannelStrategy<OUT>>,
    task: Box<Task<IN, OUT>>,
}

impl<IN, OUT> Node<IN, OUT>
    where
        IN: 'static + ArconType,
        OUT: 'static + ArconType,
{
    pub fn new(
        out_channels: Box<ChannelStrategy<OUT>>,
        task: Box<Task<IN, OUT>>,
    ) -> Node<IN, OUT> {
        Node {
            ctx: ComponentContext::new(),
            out_channels,
            task,
        }
    }
    fn handle_event(&mut self, event: &ArconEvent<IN>) -> () {
        match event {
            ArconEvent::Element(e) => {
                if let Ok(results) = self.task.handle_element(*e) {
                    for result in results {
                        self.output_event(result);
                    }
                } else {
                    error!(self.ctx.log(), "Task failed to handle element");
                }
            }
            ArconEvent::Watermark(w) => {
                if let Ok(results) = self.task.handle_watermark(*w) {
                    for result in results {
                        self.output_event(result);
                    }
                } else {
                    error!(self.ctx.log(), "Task failed to handle watermark");
                }
                self.output_event(ArconEvent::<OUT>::Watermark(*w)); // Forward the watermark
            }
        }
    }
    fn output_event(&mut self, event: ArconEvent<OUT>) -> () {
        if let Err(e) = self.out_channels.output(event, &self.ctx.system()) {
            error!(self.ctx.log(), "Node failed to send result, errror {}", e);
        }
    }
}

impl<IN, OUT> Provide<ControlPort> for Node<IN, OUT>
    where
        IN: 'static + ArconType,
        OUT: 'static + ArconType,
{
    fn handle(&mut self, event: ControlEvent) -> () {
        match event {
            ControlEvent::Start => {
            }
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
    fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
        if let Some(event) = msg.downcast_ref::<ArconEvent<IN>>() {
            self.handle_event(event);
        }
    }
    fn receive_message(&mut self, _sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {
    }
}

unsafe impl<IN, OUT> Send for Node<IN, OUT>
    where
        IN: 'static + ArconType,
        OUT: 'static + ArconType,
{}

unsafe impl<IN, OUT> Sync for Node<IN, OUT>
    where
        IN: 'static + ArconType,
        OUT: 'static + ArconType,
{}


#[cfg(test)]
mod tests {
    // Tested implicitly in integration tests of the tasks.
}

