use crate::prelude::*;
use std::collections::HashSet; // Blocked-list
use std::collections::LinkedList; // Message buffer
use std::mem;
use arcon_backend::in_memory::InMemory;
use arcon_backend::StateBackend;
use std::collections::BTreeMap; // Watermark-list
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
    watermarks: BTreeMap<String, Watermark>,
    current_watermark: u64,
    current_epoch: u64,
    blocked_channels: HashSet<String>,
    message_buffer: LinkedList<ArconMessage<IN>>,
    state_backend: Box<StateBackend>,
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
        // Initiate our watermarks
        let mut watermarks = BTreeMap::new();
        for channel in &in_channels {
            watermarks.insert(channel.clone(), Watermark{timestamp: 0});
        }

        Node {
            ctx: ComponentContext::new(),
            state_backend: Box::new(InMemory::create(&id)),
            id,
            out_channels,
            in_channels,
            task,
            watermarks,
            current_watermark: 0,
            current_epoch: 0,
            blocked_channels: HashSet::new(),
            message_buffer: LinkedList::new(),
        }
    }
    fn handle_message(&mut self, message: ArconMessage<IN>) -> ArconResult<()> {
        // Check valid sender
        if !self.in_channels.contains(&message.sender) {
            return arcon_err!("Message from invalid sender");
        }
        // Check if sender is blocked
        if self.blocked_channels.contains(&message.sender) {
            // Add the message to the back of the queue
            self.message_buffer.push_back(message);
            return Ok(());
        }

        match message.event {
            ArconEvent::Element(e) => {
                let results = self.task.handle_element(e)?;
                for result in results {
                    self.output_event(result)?;
                }
            }
            ArconEvent::Watermark(w) => {
                // Insert the watermark and try early return
                if let Some(old) = self.watermarks.insert(message.sender.clone(), w) {
                    if old.timestamp > self.current_watermark {return Ok(())} 
                }
                // A different early return
                if w.timestamp <= self.current_watermark {return Ok(())}

                // Let new_watermark take the value of the lowest watermark
                let mut new_watermark = w;
                for some_watermark in self.watermarks.values() {
                    if some_watermark.timestamp < new_watermark.timestamp { new_watermark = *some_watermark; }
                }

                // Finally, handle the watermark:
                if new_watermark.timestamp > self.current_watermark {
                    // Update the stored watermark
                    self.current_watermark = new_watermark.timestamp;
                    
                    // Handle the watermark
                    for result in self.task.handle_watermark(new_watermark)? {
                        self.output_event(result)?;
                    }
                    
                    // Forward the watermark
                    self.output_event(ArconEvent::Watermark(new_watermark))?; 
                }
            }
            ArconEvent::Epoch(e) => {
                // Add the sender to the blocked set.
                self.blocked_channels.insert(message.sender.clone());

                // If all senders blocked we can transition to new Epoch
                if self.blocked_channels.len() == self.in_channels.len() {
                    // update current epoch
                    self.current_epoch = e.epoch;

                    // call handle_epoch on our task
                    let task_state = self.task.handle_epoch(e)?;

                    // store the state
                    self.state_backend.put(b"task", &task_state);

                    // forward the epoch
                    self.output_event(ArconEvent::Epoch(e))?;

                    // flush the blocked_channels list
                    self.blocked_channels.clear();

                    // Handle the message buffer.
                    if !self.message_buffer.is_empty() {
                        // Create a new message buffer
                        let mut message_buffer = LinkedList::<ArconMessage<IN>>::new();
                        // Swap the current and the new message buffer
                        mem::swap(&mut message_buffer, &mut self.message_buffer);
                        // Iterate over the message-buffer until empty
                        while let Some(message) = message_buffer.pop_front() {
                            self.handle_message(message);
                        }
                    }
                } 
            }
        }
        Ok(())
    }
    fn output_event(&mut self, event: ArconEvent<OUT>) -> ArconResult<()> {
        let message = ArconMessage{event: event, sender: self.id.clone()};
        self.out_channels.output(message, &self.ctx.system())
    }
    fn save_state(&mut self) -> ArconResult<()> {
        let epoch = &self.current_epoch;

        Ok(())
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
        if let Some(message) = msg.downcast_ref::<ArconMessage<IN>>() {
            if let Err(err) = self.handle_message(message.clone()) {
                error!(self.ctx.log(), "Failed to handle message: {}", err);
            }
        } else {
            error!(self.ctx.log(), "Unknown message received");
        }
    }
    fn receive_message(&mut self, sender: ActorPath, ser_id: u64, buf: &mut Buf) {
        if ser_id == serialisation_ids::PBUF {
            let r = ProtoSer::deserialise(buf);
            if let Ok(msg) = r {
                if let Ok(message) = ArconMessage::from_remote(msg) {
                    if let Err(err) = self.handle_message(message) {
                        error!(self.ctx.log(), "Failed to handle message: {}", err);
                    }
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

