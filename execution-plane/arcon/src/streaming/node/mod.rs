// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

pub mod debug;
pub mod operator;

use crate::{prelude::*, state_backend::serialization::Bincode};
pub use debug::DebugNode;
use std::iter;

/*
  Node: contains an Operator which executes actions
*/
#[derive(ComponentDefinition)]
pub struct Node<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    ctx: ComponentContext<Node<IN, OUT>>,
    id: NodeID,
    out_channels: Box<dyn ChannelStrategy<OUT>>,
    in_channels: Vec<NodeID>,
    operator: Box<dyn Operator<IN, OUT> + Send>,
    watermarks: BoxedMapState<NodeID, Watermark>,
    current_watermark: BoxedValueState<Watermark>,
    current_epoch: BoxedValueState<Epoch>,
    blocked_channels: BoxedMapState<NodeID, ()>,
    message_buffer: BoxedVecState<ArconMessage<IN>>,
    state_backend: Box<dyn StateBackend>,
}

impl<IN, OUT> Node<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    pub fn new(
        id: NodeID,
        in_channels: Vec<NodeID>,
        out_channels: Box<dyn ChannelStrategy<OUT>>,
        operator: Box<dyn Operator<IN, OUT> + Send>,
        mut state_backend: Box<dyn StateBackend>,
    ) -> Node<IN, OUT> {
        // TODO: hardcoded Bincode ser/de
        // Initiate our watermarks
        let watermarks: BoxedMapState<NodeID, Watermark> =
            state_backend.new_map_state("__node_watermarks", (), (), Bincode, Bincode);

        for channel in &in_channels {
            if !watermarks
                .contains(&*state_backend, channel)
                .expect("Could not check watermarks")
            {
                watermarks
                    .fast_insert(&mut *state_backend, *channel, Watermark { timestamp: 0 })
                    .expect("Could not initialize watermarks");
            }
        }

        let current_watermark =
            state_backend.new_value_state("__node_current_watermark", (), (), Bincode, Bincode);
        if let Err(e) = current_watermark.get(&*state_backend) {
            if e.to_string().contains("Value not found") {
                current_watermark
                    .set(&mut *state_backend, Watermark { timestamp: 0 })
                    .unwrap();
            }
        }

        let current_epoch =
            state_backend.new_value_state("__node_current_epoch", (), (), Bincode, Bincode);
        if let Err(e) = current_epoch.get(&*state_backend) {
            if e.to_string().contains("Value not found") {
                current_epoch
                    .set(&mut *state_backend, Epoch { epoch: 0 })
                    .unwrap();
            }
        }

        Node {
            ctx: ComponentContext::new(),
            id,
            out_channels,
            in_channels,
            operator,
            watermarks,
            current_watermark,
            current_epoch,
            blocked_channels: state_backend.new_map_state(
                "__node_blocked_channels",
                (),
                (),
                Bincode,
                Bincode,
            ),
            message_buffer: state_backend.new_vec_state(
                "__node_message_buffer",
                (),
                (),
                Bincode,
                Bincode,
            ),
            state_backend,
        }
    }

    fn handle_message(&mut self, message: ArconMessage<IN>) -> ArconResult<()> {
        // Check valid sender
        if !self.in_channels.contains(&message.sender) {
            return arcon_err!("Message from invalid sender");
        }
        // Check if sender is blocked
        if self
            .blocked_channels
            .contains(&*self.state_backend, &message.sender)?
        {
            // Add the message to the back of the queue
            self.message_buffer
                .append(&mut *self.state_backend, message)?;
            return Ok(());
        }

        match message.event {
            ArconEvent::Element(e) => {
                let results = self.operator.handle_element(e)?;
                for result in results {
                    self.output_event(result)?;
                }
            }
            ArconEvent::Watermark(w) => {
                let current_watermark = self.current_watermark.get(&*self.state_backend)?;

                // Insert the watermark and try early return
                if let Some(old) =
                    self.watermarks
                        .insert(&mut *self.state_backend, message.sender, w)?
                {
                    if old > current_watermark {
                        return Ok(());
                    }
                }
                // A different early return
                if w <= current_watermark {
                    return Ok(());
                }

                // Let new_watermark take the value of the lowest watermark
                let new_watermark = self
                    .watermarks
                    .values(&*self.state_backend)?
                    .chain(iter::once(w))
                    .min()
                    .expect("this cannot fail, because the iterator contains at least `w`");

                // Finally, handle the watermark:
                if new_watermark > current_watermark {
                    // Update the stored watermark
                    self.current_watermark
                        .set(&mut *self.state_backend, new_watermark)?;

                    // Handle the watermark
                    for result in self.operator.handle_watermark(new_watermark)? {
                        self.output_event(result)?;
                    }

                    // Forward the watermark
                    self.output_event(ArconEvent::Watermark(new_watermark))?;
                }
            }
            ArconEvent::Epoch(e) => {
                // Add the sender to the blocked set.
                self.blocked_channels
                    .fast_insert(&mut *self.state_backend, message.sender, ())?;

                // If all senders blocked we can transition to new Epoch
                if self.blocked_channels.len(&*self.state_backend)? == self.in_channels.len() {
                    // update current epoch
                    self.current_epoch.set(&mut *self.state_backend, e)?;

                    // call handle_epoch on our operator
                    let _operator_state = self.operator.handle_epoch(e)?;

                    // store the state
                    self.save_state()?;

                    // forward the epoch
                    self.output_event(ArconEvent::Epoch(e))?;

                    // flush the blocked_channels list
                    self.blocked_channels.clear(&mut *self.state_backend)?;

                    // Handle the message buffer.
                    if !self.message_buffer.is_empty(&*self.state_backend)? {
                        // Get a local copy of the buffer
                        let local_buffer = self.message_buffer.get(&*self.state_backend)?;
                        self.message_buffer.clear(&mut *self.state_backend)?;
                        // Iterate over the message-buffer until empty
                        for message in local_buffer {
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
            event,
            sender: self.id,
        };
        self.out_channels.output(message, &self.ctx.system())
    }

    fn save_state(&mut self) -> ArconResult<()> {
        // TODO: for now we're saving to cwd, this should probably be configurable
        let checkpoint_dir = format!(
            "checkpoint_{id}_{epoch}",
            id = self.id.id,
            epoch = self.current_epoch.get(&*self.state_backend)?.epoch
        );
        self.state_backend.checkpoint(&checkpoint_dir)?;
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
    use std::{sync::Arc, thread, time};

    fn node_test_setup() -> (ActorRef<ArconMessage<i32>>, Arc<Component<DebugNode<i32>>>) {
        // Returns a filter Node with input channels: sender1..sender3
        // And a debug sink receiving its results
        let system = KompactConfig::default().build().expect("KompactSystem");

        let sink = system.create(move || DebugNode::<i32>::new());
        system.start(&sink);
        let actor_ref: ActorRefStrong<ArconMessage<i32>> =
            sink.actor_ref().hold().expect("Failed to fetch");
        let channel = Channel::Local(actor_ref);
        let channel_strategy: Box<dyn ChannelStrategy<i32>> = Box::new(Forward::new(channel));

        fn node_fn(x: &i32) -> bool {
            *x >= 0
        }

        let filter_node = system.create(move || {
            Node::<i32, i32>::new(
                0.into(),
                vec![1.into(), 2.into(), 3.into()],
                channel_strategy,
                Box::new(Filter::new(&node_fn)),
                Box::new(InMemory::new("test").unwrap()),
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
