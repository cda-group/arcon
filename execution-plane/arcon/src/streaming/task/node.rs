use crate::prelude::*;
use std::collections::BTreeMap;
use std::collections::HashSet; // Blocked-list
use std::collections::LinkedList; // Message buffer
use std::mem; // Watermark-list

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
            watermarks.insert(channel.clone(), Watermark { timestamp: 0 });
        }

        Node {
            ctx: ComponentContext::new(),
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
                    if old.timestamp > self.current_watermark {
                        return Ok(());
                    }
                }
                // A different early return
                if w.timestamp <= self.current_watermark {
                    return Ok(());
                }

                // Let new_watermark take the value of the lowest watermark
                let mut new_watermark = w;
                for some_watermark in self.watermarks.values() {
                    if some_watermark.timestamp < new_watermark.timestamp {
                        new_watermark = *some_watermark;
                    }
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
                    let _task_state = self.task.handle_epoch(e)?;

                    // store the state
                    self.save_state()?;

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
                            self.handle_message(message)?;
                        }
                    }
                }
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
    fn save_state(&mut self) -> ArconResult<()> {
        // todo
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
    // Tests the message logic of Node.
    use super::*;
    use std::sync::Arc;
    use std::{thread, time};

    // Helper functions for cleaner test-cases
    fn node_test_setup() -> (ActorRef, Arc<kompact::Component<DebugSink<i32>>>) {
        // Returns a filter Node with input channels: sender1..sender3
        // And a debug sink receiving its results
        let cfg = KompactConfig::new();
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let sink = system.create_and_start(move || DebugSink::<i32>::new());
        let channel = Channel::Local(sink.actor_ref());
        let channel_strategy: Box<ChannelStrategy<i32>> = Box::new(Forward::new(channel));

        let weld_code = String::from("|x: i32| x >= 0");
        let module = Arc::new(Module::new(weld_code).unwrap());
        let filter_node = system.create_and_start(move || {
            Node::<i32, i32>::new(
                "node1".to_string(),
                vec![
                    "sender1".to_string(),
                    "sender2".to_string(),
                    "sender3".to_string(),
                ],
                channel_strategy,
                Box::new(Filter::<i32>::new(module)),
            )
        });

        return (filter_node.actor_ref(), sink);
    }
    fn watermark(time: u64, sender: &str) -> Box<ArconMessage<i32>> {
        Box::new(ArconMessage::watermark(time, sender.to_string()))
    }
    fn element(data: i32, time: u64, sender: &str) -> Box<ArconMessage<i32>> {
        Box::new(ArconMessage::element(data, Some(time), sender.to_string()))
    }
    fn epoch(epoch: u64, sender: &str) -> Box<ArconMessage<i32>> {
        Box::new(ArconMessage::epoch(epoch, sender.to_string()))
    }
    fn wait(time: u64) -> () {
        thread::sleep(time::Duration::from_secs(time));
    }

    #[test]
    fn node_no_watermark() {
        let (node_ref, sink) = node_test_setup();
        node_ref.tell(watermark(1, "sender1"), &node_ref);

        wait(1);
        let sink_inspect = sink.definition().lock().unwrap();

        let data_len = sink_inspect.data.len();
        let watermark_len = sink_inspect.watermarks.len();
        assert_eq!(watermark_len, 0);
        assert_eq!(data_len, 0);
    }

    #[test]
    fn node_one_watermark() {
        let (node_ref, sink) = node_test_setup();
        node_ref.tell(watermark(1, "sender1"), &node_ref);
        node_ref.tell(watermark(1, "sender2"), &node_ref);
        node_ref.tell(watermark(1, "sender3"), &node_ref);

        wait(1);
        let sink_inspect = sink.definition().lock().unwrap();

        let data_len = sink_inspect.data.len();
        let watermark_len = sink_inspect.watermarks.len();
        assert_eq!(watermark_len, 1);
        assert_eq!(data_len, 0);
    }
    #[test]
    fn node_outoforder_watermarks() {
        let (node_ref, sink) = node_test_setup();
        node_ref.tell(watermark(1, "sender1"), &node_ref);
        node_ref.tell(watermark(3, "sender1"), &node_ref);
        node_ref.tell(watermark(1, "sender2"), &node_ref);
        node_ref.tell(watermark(2, "sender2"), &node_ref);
        node_ref.tell(watermark(4, "sender3"), &node_ref);

        wait(1);
        let sink_inspect = sink.definition().lock().unwrap();

        let watermark_len = sink_inspect.watermarks.len();
        assert_eq!(watermark_len, 1);
        assert_eq!(sink_inspect.watermarks[0].timestamp, 2u64);
    }
    #[test]
    fn node_epoch_block() {
        let (node_ref, sink) = node_test_setup();
        node_ref.tell(element(1, 1, "sender1"), &node_ref);
        node_ref.tell(epoch(3, "sender1"), &node_ref);
        // should be blocked:
        node_ref.tell(element(2, 1, "sender1"), &node_ref);
        // should not be blocked
        node_ref.tell(element(3, 1, "sender2"), &node_ref);

        wait(1);
        let sink_inspect = sink.definition().lock().unwrap();

        let data_len = sink_inspect.data.len();
        let epoch_len = sink_inspect.epochs.len();
        assert_eq!(epoch_len, 0);
        assert_eq!(sink_inspect.data[0].data, 1i32);
        assert_eq!(sink_inspect.data[1].data, 3i32);
        assert_eq!(data_len, 2);
    }
    #[test]
    fn node_epoch_no_continue() {
        let (node_ref, sink) = node_test_setup();
        node_ref.tell(element(11, 1, "sender1"), &node_ref); // not blocked
        node_ref.tell(epoch(1, "sender1"), &node_ref); // sender1 blocked
        node_ref.tell(element(12, 1, "sender1"), &node_ref); // blocked
        node_ref.tell(element(21, 1, "sender2"), &node_ref); // not blocked
        node_ref.tell(epoch(2, "sender1"), &node_ref); // blocked
        node_ref.tell(epoch(1, "sender2"), &node_ref); // sender2 blocked
        node_ref.tell(epoch(2, "sender2"), &node_ref); // blocked
        node_ref.tell(element(23, 1, "sender2"), &node_ref); // blocked
        node_ref.tell(element(31, 1, "sender3"), &node_ref); // not blocked

        wait(1);
        let sink_inspect = sink.definition().lock().unwrap();

        let data_len = sink_inspect.data.len();
        let epoch_len = sink_inspect.epochs.len();
        assert_eq!(epoch_len, 0); // no epochs should've completed
        assert_eq!(sink_inspect.data[0].data, 11i32);
        assert_eq!(sink_inspect.data[1].data, 21i32);
        assert_eq!(sink_inspect.data[2].data, 31i32);
        assert_eq!(data_len, 3);
    }
    #[test]
    fn node_epoch_continue() {
        // Same test as previous but we finnish it by sending the required epochs
        let (node_ref, sink) = node_test_setup();
        node_ref.tell(element(11, 1, "sender1"), &node_ref); // not blocked
        node_ref.tell(epoch(1, "sender1"), &node_ref); // sender1 blocked
        node_ref.tell(element(12, 1, "sender1"), &node_ref); // blocked
        node_ref.tell(element(21, 1, "sender2"), &node_ref); // not blocked
        node_ref.tell(epoch(2, "sender1"), &node_ref); // blocked
        node_ref.tell(element(13, 1, "sender1"), &node_ref); // blocked
        node_ref.tell(epoch(1, "sender2"), &node_ref); // sender2 blocked
        node_ref.tell(epoch(2, "sender2"), &node_ref); // blocked
        node_ref.tell(element(22, 1, "sender2"), &node_ref); // blocked
        node_ref.tell(element(31, 1, "sender3"), &node_ref); // not blocked
        node_ref.tell(epoch(1, "sender3"), &node_ref); // Complete our epochs
        node_ref.tell(epoch(2, "sender3"), &node_ref);
        // All the elements should now have been delivered in specific order

        wait(1);
        let sink_inspect = sink.definition().lock().unwrap();

        let data_len = sink_inspect.data.len();
        let epoch_len = sink_inspect.epochs.len();
        assert_eq!(epoch_len, 2); // 3 epochs should've completed
        assert_eq!(sink_inspect.data[0].data, 11i32);
        assert_eq!(sink_inspect.data[1].data, 21i32);
        assert_eq!(sink_inspect.data[2].data, 31i32);
        assert_eq!(sink_inspect.data[3].data, 12i32); // First message in epoch1
        assert_eq!(sink_inspect.data[4].data, 13i32); // First message in epoch2
        assert_eq!(sink_inspect.data[5].data, 22i32); // 2nd message in epoch2
        assert_eq!(data_len, 6);
    }
}
