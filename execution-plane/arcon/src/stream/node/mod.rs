// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

/// Debug version of [Node]
pub mod debug;

use crate::{
    manager::node_manager::*,
    metrics::{counter::Counter, gauge::Gauge, meter::Meter},
    prelude::*,
    stream::operator::OperatorContext,
    timer::TimerBackend,
};
use std::iter;

/// Type alias for a Node description
pub type NodeDescriptor = String;

/// Metrics reported by an Arcon Node
#[derive(Debug, Clone)]
pub struct NodeMetrics {
    /// Meter reporting inbound throughput
    pub inbound_throughput: Meter,
    /// Counter for total epochs processed
    pub epoch_counter: Counter,
    /// Counter for total watermarks processed
    pub watermark_counter: Counter,
    /// Current watermark
    pub watermark: Watermark,
    /// Current epoch
    pub epoch: Epoch,
    /// Gauge Metric representing number of outbound channels
    pub outbound_channels: Gauge,
    /// Gauge Metric representing number of inbound channels
    pub inbound_channels: Gauge,
}

impl NodeMetrics {
    /// Creates a NodeMetrics struct
    pub fn new() -> NodeMetrics {
        NodeMetrics {
            inbound_throughput: Meter::new(),
            epoch_counter: Counter::new(),
            watermark_counter: Counter::new(),
            watermark: Watermark::new(0),
            epoch: Epoch::new(0),
            outbound_channels: Gauge::new(),
            inbound_channels: Gauge::new(),
        }
    }
}

/// Internal Node State
pub struct NodeState<OP: Operator> {
    /// Mappings from NodeID to current Watermark
    watermarks: BoxedMapState<NodeID, Watermark>,
    /// Current Watermark value
    current_watermark: BoxedValueState<Watermark>,
    /// Current Epoch
    current_epoch: BoxedValueState<Epoch>,
    /// Blocked channels during epoch alignment
    blocked_channels: BoxedMapState<NodeID, ()>,
    /// Temporary message buffer used while having blocked channels
    message_buffer: BoxedVecState<ArconMessage<OP::IN>>,
}

/// A Node is a [kompact] component that drives the execution of streaming operators
///
/// Nodes receive [ArconMessage] and run some transform on the data
/// before using a [ChannelStrategy] to send the result to another Node.
#[derive(ComponentDefinition)]
pub struct Node<OP>
where
    OP: Operator + 'static,
{
    /// Component context
    ctx: ComponentContext<Self>,
    /// Port for NodeManager
    pub(crate) node_manager_port: RequiredPort<NodeManagerPort, Self>,
    /// Node descriptor
    descriptor: NodeDescriptor,
    /// A Node identifier
    id: NodeID,
    /// Strategy used by the Node
    channel_strategy: ChannelStrategy<OP::OUT>,
    /// Current set of IDs connected to this Node
    in_channels: Vec<NodeID>,
    /// User-defined Operator
    operator: OP,
    /// Internal state of the Node
    state: NodeState<OP>,
    /// Metrics collected by the Node
    metrics: NodeMetrics,
    /// State Backend used to persist data
    pub state_backend: Box<dyn StateBackend>,
    /// Timer Backend to keep track of event timers
    timer_backend: Box<dyn TimerBackend<OP::TimerState>>,
}

// Just a shorthand to avoid repeating the OperatorContext construction everywhere
macro_rules! make_context {
    ($sel:ident) => {
        OperatorContext::new(
            &mut $sel.channel_strategy,
            $sel.state_backend.as_mut(),
            $sel.timer_backend.as_mut(),
        )
    };
}

impl<OP> Node<OP>
where
    OP: Operator + 'static,
{
    /// Creates a new Node
    pub fn new<F>(
        descriptor: NodeDescriptor,
        id: NodeID,
        in_channels: Vec<NodeID>,
        channel_strategy: ChannelStrategy<OP::OUT>,
        mut operator: OP,
        mut state_backend: Box<dyn StateBackend>,
        timer_backend_fn: F,
    ) -> Self
    where
        F: Fn(&mut dyn StateBackend) -> Box<dyn TimerBackend<OP::TimerState>> + Sized + 'static,
    {
        let sb_session = state_backend.new_session();
        // Initiate our watermarks

        // some backends require you to first specify all the states and mess with them later
        // declare
        let watermarks = state_backend.build("__node_watermarks").map();
        let current_watermark = state_backend.build("__node_current_watermark").value();
        let current_epoch = state_backend.build("__node_current_epoch").value();
        let blocked_channels = state_backend.build("__node_blocked_channels").map();
        let message_buffer = state_backend.build("__node_message_buffer").vec();

        // timer backends may declare their own state
        // so it must initialised here before the first access
        let timer_backend = timer_backend_fn(state_backend.as_mut());

        // initialize
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

        if current_watermark
            .get(&*state_backend)
            .expect("watermark get error")
            .is_none()
        {
            current_watermark
                .set(&mut *state_backend, Watermark { timestamp: 0 })
                .unwrap();
        }

        if current_epoch
            .get(&*state_backend)
            .expect("current epoch get error")
            .is_none()
        {
            current_epoch
                .set(&mut *state_backend, Epoch { epoch: 0 })
                .unwrap();
        }

        operator.init(state_backend.as_mut());

        let state = NodeState {
            watermarks,
            current_watermark,
            current_epoch,
            blocked_channels,
            message_buffer,
        };

        // Set up metrics structure
        let mut metrics = NodeMetrics::new();
        metrics.inbound_channels.inc_n(in_channels.len());
        metrics
            .outbound_channels
            .inc_n(channel_strategy.num_channels());

        drop(sb_session);

        Node {
            ctx: ComponentContext::new(),
            node_manager_port: RequiredPort::new(),
            descriptor,
            id,
            channel_strategy,
            in_channels,
            operator,
            state,
            metrics,
            state_backend,
            timer_backend,
        }
    }

    #[inline]
    fn handle_message(&mut self, message: ArconMessage<OP::IN>) -> ArconResult<()> {
        let sb_session = self.state_backend.new_session();

        // Check valid sender
        if !self.in_channels.contains(&message.sender) {
            return arcon_err!("Message from invalid sender");
        }

        // Mark amount of inbound messages
        self.metrics
            .inbound_throughput
            .mark_n(message.events.len() as u64);

        // Check if sender is blocked
        if self
            .state
            .blocked_channels
            .contains(&*self.state_backend, &message.sender)?
        {
            // Add the message to the back of the queue
            self.state
                .message_buffer
                .append(&mut *self.state_backend, message)?;
            return Ok(());
        }

        'event_loop: for event in message.events {
            match event.unwrap() {
                ArconEvent::Element(e) => {
                    if e.timestamp.unwrap_or(u64::max_value())
                        <= self
                            .state
                            .watermarks
                            .get(&*self.state_backend, &message.sender)?
                            .ok_or_else(|| arcon_err_kind!("uninitialized watermark"))?
                            .timestamp
                    {
                        continue 'event_loop;
                    }

                    self.operator.handle_element(e, make_context!(self));
                }
                ArconEvent::Watermark(w) => {
                    if w <= self
                        .state
                        .watermarks
                        .get(&*self.state_backend, &message.sender)?
                        .ok_or_else(|| arcon_err_kind!("uninitialized watermark"))?
                    {
                        continue 'event_loop;
                    }

                    let current_watermark = self
                        .state
                        .current_watermark
                        .get(&*self.state_backend)?
                        .ok_or_else(|| arcon_err_kind!("current watermark uninitialized"))?;

                    // Insert the watermark and try early return
                    if let Some(old) =
                        self.state
                            .watermarks
                            .insert(&mut *self.state_backend, message.sender, w)?
                    {
                        if old > current_watermark {
                            continue 'event_loop;
                        }
                    }
                    // A different early return
                    if w <= current_watermark {
                        continue 'event_loop;
                    }

                    // Let new_watermark take the value of the lowest watermark
                    let new_watermark = self
                        .state
                        .watermarks
                        .values(&*self.state_backend)?
                        .chain(iter::once(Ok(w)))
                        .min_by(|res_x, res_y| match (res_x, res_y) {
                            // if both watermarks are successfully fetched, compare them
                            (Ok(x), Ok(y)) => x.cmp(y),
                            // otherwise prefer errors
                            (Err(_), _) => std::cmp::Ordering::Less,
                            (_, Err(_)) => std::cmp::Ordering::Greater,
                        })
                        .expect(
                            "this cannot fail, because the iterator contains at least `Ok(w)`",
                        )?;

                    // Finally, handle the watermark:
                    if new_watermark > current_watermark {
                        // Update the stored watermark
                        self.state
                            .current_watermark
                            .set(&mut *self.state_backend, new_watermark)?;

                        // Handle the watermark
                        self.operator
                            .handle_watermark(new_watermark, make_context!(self));

                        for timeout in self
                            .timer_backend
                            .advance_to(new_watermark.timestamp, self.state_backend.as_mut())
                        {
                            self.operator.handle_timeout(timeout, make_context!(self));
                        }

                        // Set current watermark
                        self.metrics.watermark = new_watermark;

                        // Forward the watermark
                        self.channel_strategy
                            .add(ArconEvent::Watermark(new_watermark));

                        // increment watermark counter
                        self.metrics.watermark_counter.inc();
                    }
                }
                ArconEvent::Epoch(e) => {
                    if e <= self
                        .state
                        .current_epoch
                        .get(&*self.state_backend)?
                        .ok_or_else(|| arcon_err_kind!("uninitialized epoch"))?
                    {
                        continue 'event_loop;
                    }

                    // Add the sender to the blocked set.
                    self.state.blocked_channels.fast_insert(
                        &mut *self.state_backend,
                        message.sender,
                        (),
                    )?;

                    // If all senders blocked we can transition to new Epoch
                    if self.state.blocked_channels.len(&*self.state_backend)?
                        == self.in_channels.len()
                    {
                        // update current epoch
                        self.state.current_epoch.set(&mut *self.state_backend, e)?;

                        self.operator.handle_epoch(e, make_context!(self));
                        self.timer_backend
                            .handle_epoch(e, self.state_backend.as_mut());

                        // store the state
                        self.save_state()?;

                        // Set current epoch
                        self.metrics.epoch = e;

                        // forward the epoch
                        self.channel_strategy.add(ArconEvent::Epoch(e));

                        // increment epoch counter
                        self.metrics.epoch_counter.inc();

                        self.after_state_save()?;
                    }
                }
                ArconEvent::Death(s) => {
                    // We are instructed to shutdown....
                    self.channel_strategy.add(ArconEvent::Death(s));
                    // TODO: invoke shutdown operations...
                }
            }
        }

        drop(sb_session);

        Ok(())
    }

    fn save_state(&mut self) -> ArconResult<()> {
        if let Some(base_dir) = &self.ctx.config()["checkpoint_dir"].as_string() {
            let checkpoint_dir = format!(
                "{}/checkpoint_{id}_{epoch}",
                base_dir,
                id = self.id.id,
                epoch = self
                    .state
                    .current_epoch
                    .get(&*self.state_backend)?
                    .ok_or_else(|| arcon_err_kind!("current epoch uninitialized"))?
                    .epoch
            );
            self.state_backend.checkpoint(checkpoint_dir.as_ref())?;
            debug!(
                self.ctx.log(),
                "Completed a Checkpoint to path {}", checkpoint_dir
            );
        } else {
            return arcon_err!("Failed to fetch checkpoint_dir from Config");
        }

        Ok(())
    }

    fn after_state_save(&mut self) -> ArconResult<()> {
        // flush the blocked_channels list
        self.state
            .blocked_channels
            .clear(&mut *self.state_backend)?;

        let message_buffer = &mut self.state.message_buffer;

        // Handle the message buffer.
        if !message_buffer.is_empty(&*self.state_backend)? {
            // Get a local copy of the buffer
            let local_buffer = message_buffer.get(&*self.state_backend)?;
            message_buffer.clear(&mut *self.state_backend)?;

            // Iterate over the message-buffer until empty
            for message in local_buffer {
                self.handle_message(message)?;
            }
        }

        Ok(())
    }
}

impl<OP> Provide<ControlPort> for Node<OP>
where
    OP: Operator + 'static,
{
    fn handle(&mut self, event: ControlEvent) {
        match event {
            ControlEvent::Start => {
                debug!(
                    self.ctx.log(),
                    "Started Arcon Node {} with Node ID {:?}", self.descriptor, self.id
                );

                // Start periodic timer reporting Node metrics
                if let Some(interval) = &self.ctx().config()["node_metrics_interval"].as_i64() {
                    let time_dur = std::time::Duration::from_millis(*interval as u64);
                    self.schedule_periodic(time_dur, time_dur, |c_self, _id| {
                        c_self
                            .node_manager_port
                            .trigger(NodeEvent::Metrics(c_self.id, c_self.metrics.clone()));
                    });
                }

                if self.state_backend.was_restored() {
                    if let Err(e) = self.after_state_save() {
                        error!(self.ctx.log(), "restoration error: {}", e);
                    }
                }
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

impl<OP> Require<NodeManagerPort> for Node<OP>
where
    OP: Operator + 'static,
{
    fn handle(&mut self, _: Never) {
        unreachable!("Never can't be instantiated!");
    }
}
impl<OP> Provide<NodeManagerPort> for Node<OP>
where
    OP: Operator + 'static,
{
    fn handle(&mut self, e: NodeEvent) {
        trace!(self.log(), "Ignoring node event: {:?}", e)
    }
}

impl<OP> Actor for Node<OP>
where
    OP: Operator + 'static,
{
    type Message = ArconMessage<OP::IN>;

    fn receive_local(&mut self, msg: Self::Message) {
        if let Err(err) = self.handle_message(msg) {
            error!(self.ctx.log(), "Failed to handle message: {}", err);
        }
    }
    fn receive_network(&mut self, msg: NetMessage) {
        let arcon_msg = match *msg.ser_id() {
            ReliableSerde::<OP::IN>::SER_ID => msg
                .try_deserialise::<ArconMessage<OP::IN>, ReliableSerde<OP::IN>>()
                .map_err(|_| arcon_err_kind!("Failed to unpack reliable ArconMessage")),
            UnsafeSerde::<OP::IN>::SER_ID => msg
                .try_deserialise::<ArconMessage<OP::IN>, UnsafeSerde<OP::IN>>()
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
    use crate::{pipeline::*, state_backend::in_memory::InMemory, timer};
    use std::{sync::Arc, thread, time};

    fn node_test_setup() -> (ActorRef<ArconMessage<i32>>, Arc<Component<DebugNode<i32>>>) {
        // Returns a filter Node with input channels: sender1..sender3
        // And a debug sink receiving its results
        let mut pipeline = ArconPipeline::new();
        let system = &pipeline.system();

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
            Node::new(
                String::from("filter_node"),
                0.into(),
                vec![1.into(), 2.into(), 3.into()],
                channel_strategy,
                Filter::new(&node_fn),
                Box::new(InMemory::new("test".as_ref()).unwrap()),
                timer::none,
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
    fn death(sender: u32) -> ArconMessage<i32> {
        ArconMessage::death(String::from("die"), sender.into())
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

        node_ref.tell(death(2)); // send death marker on unblocked channel to flush

        wait(1);
        let sink_inspect = sink.definition().lock().unwrap();

        let data_len = sink_inspect.data.len();
        let epoch_len = sink_inspect.epochs.len();
        assert_eq!(epoch_len, 0);
        assert_eq!(sink_inspect.data[0].data, Some(1i32));
        assert_eq!(sink_inspect.data[1].data, Some(3i32));
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

        node_ref.tell(death(3)); // send death marker on unblocked channel to flush
        wait(1);
        let sink_inspect = sink.definition().lock().unwrap();

        let data_len = sink_inspect.data.len();
        let epoch_len = sink_inspect.epochs.len();
        assert_eq!(epoch_len, 0); // no epochs should've completed
        assert_eq!(sink_inspect.data[0].data, Some(11i32));
        assert_eq!(sink_inspect.data[1].data, Some(21i32));
        assert_eq!(sink_inspect.data[2].data, Some(31i32));
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

        node_ref.tell(death(3)); // send death marker on unblocked channel to flush
        wait(1);
        let sink_inspect = sink.definition().lock().unwrap();

        let data_len = sink_inspect.data.len();
        let epoch_len = sink_inspect.epochs.len();
        assert_eq!(epoch_len, 2); // 3 epochs should've completed
        assert_eq!(sink_inspect.data[0].data, Some(11i32));
        assert_eq!(sink_inspect.data[1].data, Some(21i32));
        assert_eq!(sink_inspect.data[2].data, Some(31i32));
        assert_eq!(sink_inspect.data[3].data, Some(12i32)); // First message in epoch1
        assert_eq!(sink_inspect.data[4].data, Some(13i32)); // First message in epoch2
        assert_eq!(sink_inspect.data[5].data, Some(22i32)); // 2nd message in epoch2
        assert_eq!(data_len, 6);
    }
}
