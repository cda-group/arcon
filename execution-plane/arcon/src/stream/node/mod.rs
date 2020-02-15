// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

/// Debug version of [Node]
pub mod debug;

use crate::prelude::*;
use std::collections::BTreeMap;
use std::collections::HashSet; // Blocked-list
use std::collections::LinkedList; // Message buffer
use std::mem; // Watermark-list

/// A Node is a [kompact] component that drives the execution of streaming operators
///
/// Nodes receive [ArconMessage] and run some transform on the data
/// before using a [ChannelStrategy] to send the result to another Node.
#[derive(ComponentDefinition)]
pub struct Node<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    ctx: ComponentContext<Node<IN, OUT>>,
    _id: NodeID,
    channel_strategy: ChannelStrategy<OUT>,
    in_channels: Vec<NodeID>,
    operator: Box<dyn Operator<IN, OUT> + Send>,
    watermarks: BTreeMap<NodeID, Watermark>,
    current_watermark: u64,
    current_epoch: u64,
    blocked_channels: HashSet<NodeID>,
    message_buffer: LinkedList<ArconMessage<IN>>,
}

impl<IN, OUT> Node<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    pub fn new(
        id: NodeID,
        in_channels: Vec<NodeID>,
        channel_strategy: ChannelStrategy<OUT>,
        operator: Box<dyn Operator<IN, OUT> + Send>,
    ) -> Node<IN, OUT> {
        // Initiate our watermarks
        let mut watermarks = BTreeMap::new();
        for channel in &in_channels {
            watermarks.insert(*channel, Watermark { timestamp: 0 });
        }

        Node {
            ctx: ComponentContext::new(),
            _id: id,
            channel_strategy,
            in_channels,
            operator,
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

        for event in message.events.into_iter() {
            match event {
                ArconEvent::Element(e) => {
                    if let Some(events) = self.operator.handle_element(e) {
                        for new_event in events {
                            self.channel_strategy.add(new_event);
                        }
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
                        if let Some(wm_output) = self.operator.handle_watermark(new_watermark) {
                            for event in wm_output {
                                self.channel_strategy.add(event);
                            }
                        }

                        // Forward the watermark
                        // NOTE: Flush channel buffer directly as not to slow down event-time triggers
                        self.channel_strategy.add_and_flush(
                            ArconEvent::Watermark(new_watermark),
                            &self.ctx().system(),
                        );
                    }
                }
                ArconEvent::Epoch(e) => {
                    // Add the sender to the blocked set.
                    self.blocked_channels.insert(message.sender.clone());

                    // If all senders blocked we can transition to new Epoch
                    if self.blocked_channels.len() == self.in_channels.len() {
                        // update current epoch
                        self.current_epoch = e.epoch;

                        // call handle_epoch on our operator
                        let _operator_state = self.operator.handle_epoch(e);

                        // store the state
                        self.save_state()?;

                        // forward the epoch
                        self.channel_strategy.add(ArconEvent::Epoch(e));

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
            self.channel_strategy.flush(&self.ctx().system());
        }

        Ok(())
    }

    fn save_state(&mut self) -> ArconResult<()> {
        // todo
        Ok(())
    }
}

impl<IN, OUT> Provide<ControlPort> for Node<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    fn handle(&mut self, event: ControlEvent) {
        match event {
            ControlEvent::Start => {
                debug!(self.ctx.log(), "Started Arcon Node");
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

impl<IN, OUT> Actor for Node<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    type Message = ArconMessage<IN>;

    fn receive_local(&mut self, msg: Self::Message) {
        if let Err(err) = self.handle_message(msg) {
            error!(self.ctx.log(), "Failed to handle message: {}", err);
        }
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
                if let Err(err) = self.handle_message(m) {
                    error!(self.ctx.log(), "Failed to handle node message: {}", err);
                }
            }
            Err(e) => error!(self.ctx.log(), "Error ArconNetworkMessage: {:?}", e),
        }
    }
}

#[cfg(test)]
mod tests {
    // Tests the message logic of Node.
    use super::*;
    use std::sync::Arc;
    use std::{thread, time};

    fn node_test_setup() -> (ActorRef<ArconMessage<i32>>, Arc<Component<DebugNode<i32>>>) {
        // Returns a filter Node with input channels: sender1..sender3
        // And a debug sink receiving its results
        let system = KompactConfig::default().build().expect("KompactSystem");

        let sink = system.create(move || DebugNode::<i32>::new());
        system.start(&sink);
        let actor_ref: ActorRefStrong<ArconMessage<i32>> =
            sink.actor_ref().hold().expect("Failed to fetch");
        let channel = Channel::Local(actor_ref);
        let channel_strategy: ChannelStrategy<i32> =
            ChannelStrategy::Forward(Forward::new(channel, NodeID::new(0)));

        fn node_fn(x: &i32) -> bool {
            *x >= 0
        }

        let filter_node = system.create(move || {
            Node::<i32, i32>::new(
                0.into(),
                vec![1.into(), 2.into(), 3.into()],
                channel_strategy,
                Box::new(Filter::new(&node_fn)),
            )
        });

        system.start(&filter_node);

        return (filter_node.actor_ref(), sink);
    }

    fn watermark(time: u64, sender: u32) -> ArconMessage<i32> {
        ArconMessage::watermark(time, sender.into())
    }

    fn element(data: i32, time: u64, sender: u32) -> ArconMessage<i32> {
        ArconMessage::element(data, Some(time), sender.into())
    }

    fn epoch(epoch: u64, sender: u32) -> ArconMessage<i32> {
        ArconMessage::epoch(epoch, sender.into())
    }

    fn wait(time: u64) {
        thread::sleep(time::Duration::from_secs(time));
    }

    #[test]
    fn node_no_watermark() {
        let (node_ref, sink) = node_test_setup();
        node_ref.tell(watermark(1, 1));

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
        node_ref.tell(watermark(1, 1));
        node_ref.tell(watermark(1, 2));
        node_ref.tell(watermark(1, 3));

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
        node_ref.tell(watermark(1, 1));
        node_ref.tell(watermark(3, 1));
        node_ref.tell(watermark(1, 2));
        node_ref.tell(watermark(2, 2));
        node_ref.tell(watermark(4, 3));

        wait(1);
        let sink_inspect = sink.definition().lock().unwrap();

        let watermark_len = sink_inspect.watermarks.len();
        assert_eq!(watermark_len, 1);
        assert_eq!(sink_inspect.watermarks[0].timestamp, 2u64);
    }

    #[test]
    fn node_epoch_block() {
        let (node_ref, sink) = node_test_setup();
        node_ref.tell(element(1, 1, 1));
        node_ref.tell(epoch(3, 1));
        // should be blocked:
        node_ref.tell(element(2, 1, 1));
        // should not be blocked
        node_ref.tell(element(3, 1, 2));

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
        node_ref.tell(element(11, 1, 1)); // not blocked
        node_ref.tell(epoch(1, 1)); // sender1 blocked
        node_ref.tell(element(12, 1, 1)); // blocked
        node_ref.tell(element(21, 1, 2)); // not blocked
        node_ref.tell(epoch(2, 1)); // blocked
        node_ref.tell(epoch(1, 2)); // sender2 blocked
        node_ref.tell(epoch(2, 2)); // blocked
        node_ref.tell(element(23, 1, 2)); // blocked
        node_ref.tell(element(31, 1, 3)); // not blocked

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
        node_ref.tell(element(11, 1, 1)); // not blocked
        node_ref.tell(epoch(1, 1)); // sender1 blocked
        node_ref.tell(element(12, 1, 1)); // blocked
        node_ref.tell(element(21, 1, 2)); // not blocked
        node_ref.tell(epoch(2, 1)); // blocked
        node_ref.tell(element(13, 1, 1)); // blocked
        node_ref.tell(epoch(1, 2)); // sender2 blocked
        node_ref.tell(epoch(2, 2)); // blocked
        node_ref.tell(element(22, 1, 2)); // blocked
        node_ref.tell(element(31, 1, 3)); // not blocked
        node_ref.tell(epoch(1, 3)); // Complete our epochs
        node_ref.tell(epoch(2, 3));
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
