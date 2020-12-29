// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

/// Debug version of [Node]
pub mod debug;

pub mod source;

#[cfg(feature = "unsafe_flight")]
use crate::data::flight_serde::unsafe_remote::UnsafeSerde;
use crate::{
    data::{flight_serde::reliable_remote::ReliableSerde, RawArconMessage, *},
    manager::node::{NodeEvent::Checkpoint, *},
    stream::{
        channel::strategy::ChannelStrategy,
        operator::{Operator, OperatorContext},
    },
};
use arcon_error::{arcon_err, arcon_err_kind, ArconResult};
use arcon_state::{index::IndexOps, Appender, ArconState, Backend, Handle, Timer as ArconTimer}; // conflicts with Kompact Timer trait
use fxhash::*;
use kompact::prelude::*;
use std::{cell::UnsafeCell, sync::Arc};

#[cfg(feature = "metrics")]
use crate::metrics::{counter::Counter, gauge::Gauge, meter::Meter};

const MESSAGE_BUFFER_SIZE: usize = 1024;

/// Type alias for a Node description
pub type NodeDescriptor = String;

#[cfg(feature = "metrics")]
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

#[cfg(feature = "metrics")]
impl Default for NodeMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "metrics")]
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

#[derive(ArconState)]
pub struct NodeState<OP: Operator + 'static, B: Backend> {
    /// Durable message buffer used for blocked channels
    message_buffer: Appender<RawArconMessage<OP::IN>, B>,
    /// Map of senders and their corresponding Watermark
    #[ephemeral]
    watermarks: FxHashMap<NodeID, Watermark>,
    /// Map of blocked senders
    #[ephemeral]
    blocked_channels: FxHashSet<NodeID>,
    /// Current Watermark value for the Node
    #[ephemeral]
    current_watermark: Watermark,
    /// Current Epoch value for the Node
    #[ephemeral]
    current_epoch: Epoch,
    /// Vector of expected senders
    ///
    /// Used to validate message and separate channels
    #[ephemeral]
    in_channels: Vec<NodeID>,
    /// Identifier for the Node
    #[ephemeral]
    id: NodeID,
}

impl<OP: Operator + 'static, B: Backend> NodeState<OP, B> {
    pub fn new(id: NodeID, in_channels: Vec<NodeID>, backend: Arc<B>) -> Self {
        let mut handle = Handle::vec("_messagebuffer");
        backend.register_vec_handle(&mut handle);
        let active_handle = handle.activate(backend);
        let message_buffer = Appender::with_capacity(MESSAGE_BUFFER_SIZE, active_handle);

        // initialise watermarks
        let mut watermarks: FxHashMap<NodeID, Watermark> = FxHashMap::default();
        for sender in &in_channels {
            watermarks.insert(*sender, Watermark::new(0));
        }

        Self {
            message_buffer,
            watermarks,
            blocked_channels: FxHashSet::default(),
            current_watermark: Watermark::new(0),
            current_epoch: Epoch::new(0),
            in_channels,
            id,
        }
    }
}

// Just a shorthand to avoid repeating the OperatorContext construction everywhere
macro_rules! make_context {
    ($sel:ident) => {
        OperatorContext::new(
            $sel,
            &mut (*$sel.timer.get()),
            &mut (*$sel.channel_strategy.get()),
        )
    };
}

/// A Node is a [kompact] component that drives the execution of streaming operators
#[derive(ComponentDefinition)]
pub struct Node<OP, B>
where
    OP: Operator + 'static,
    B: Backend,
{
    /// Component context
    ctx: ComponentContext<Self>,
    /// Port for NodeManager
    pub(crate) node_manager_port: RequiredPort<NodeManagerPort>,
    /// Node descriptor
    descriptor: NodeDescriptor,
    /// Channel Strategy used by the Node
    channel_strategy: UnsafeCell<ChannelStrategy<OP::OUT>>,
    /// User-defined Operator
    operator: UnsafeCell<OP>,
    #[cfg(feature = "metrics")]
    /// Metrics collected by the Node
    metrics: NodeMetrics,
    /// Internal Node State
    node_state: NodeState<OP, B>,
    /// Event time scheduler
    timer: UnsafeCell<Option<ArconTimer<u64, OP::TimerState, B>>>,

    checkpoint_request: Option<Arc<CheckpointRequest>>,
}

impl<OP, B> Node<OP, B>
where
    OP: Operator + 'static,
    B: Backend,
{
    /// Creates a new Node
    #[cfg(test)]
    pub(crate) fn new(
        descriptor: NodeDescriptor,
        channel_strategy: ChannelStrategy<OP::OUT>,
        operator: OP,
        node_state: NodeState<OP, B>,
    ) -> Self {
        Self::setup(descriptor, channel_strategy, operator, node_state, None)
    }

    /// Creates a new Node with timer
    pub fn with_timer(
        descriptor: NodeDescriptor,
        channel_strategy: ChannelStrategy<OP::OUT>,
        operator: OP,
        node_state: NodeState<OP, B>,
        timer: ArconTimer<u64, OP::TimerState, B>,
    ) -> Self {
        Self::setup(
            descriptor,
            channel_strategy,
            operator,
            node_state,
            Some(timer),
        )
    }

    fn setup(
        descriptor: NodeDescriptor,
        channel_strategy: ChannelStrategy<OP::OUT>,
        operator: OP,
        node_state: NodeState<OP, B>,
        timer: Option<ArconTimer<u64, OP::TimerState, B>>,
    ) -> Self {
        Node {
            ctx: ComponentContext::uninitialised(),
            node_manager_port: RequiredPort::uninitialised(),
            descriptor,
            channel_strategy: UnsafeCell::new(channel_strategy),
            operator: UnsafeCell::new(operator),
            #[cfg(feature = "metrics")]
            metrics: NodeMetrics::new(),
            node_state,
            timer: UnsafeCell::new(timer),
            checkpoint_request: None,
        }
    }

    /// Handle a Raw ArconMessage that has either been sent remotely or temporarily stored in the state backend
    #[inline]
    fn handle_raw_msg(&mut self, message: RawArconMessage<OP::IN>) -> ArconResult<()> {
        if !self.node_state.in_channels.contains(&message.sender) {
            return arcon_err!("Message from invalid sender");
        }

        if self.sender_blocked(&message.sender) {
            if let Some(req) = &self.checkpoint_request {
                if req.is_complete() {
                    self.complete_epoch()?;
                    // epoch finalised, handle events as normal..
                    return self.handle_events(message.sender, message.events);
                }
            }

            // Add the message to the back of the queue
            self.node_state.message_buffer().append(message)?;
            return Ok(());
        }

        #[cfg(feature = "metrics")]
        self.record_incoming_events(message.events.len() as u64);

        self.handle_events(message.sender, message.events)
    }

    /// Handle a local ArconMessage that is backed by the arcon allocator
    #[inline]
    fn handle_message(&mut self, message: ArconMessage<OP::IN>) -> ArconResult<()> {
        if !self.node_state.in_channels.contains(&message.sender) {
            return arcon_err!("Message from invalid sender");
        }

        if self.sender_blocked(&message.sender) {
            if let Some(req) = &mut self.checkpoint_request {
                if req.is_complete() {
                    self.complete_epoch()?;
                    // epoch finalised, handle events as normal..
                    return self.handle_events(message.sender, message.events);
                }
            }
            // Add the message to the back of the queue
            self.node_state.message_buffer().append(message.into())?;
            return Ok(());
        }

        #[cfg(feature = "metrics")]
        self.record_incoming_events(message.events.len() as u64);

        self.handle_events(message.sender, message.events)
    }

    #[inline(always)]
    fn sender_blocked(&mut self, sender: &NodeID) -> bool {
        self.node_state.blocked_channels().contains(sender)
    }

    #[cfg(feature = "metrics")]
    /// Mark amount of inbound events
    #[inline(always)]
    fn record_incoming_events(&mut self, total: u64) {
        self.metrics.inbound_throughput.mark_n(total);
    }

    /// Iterate over a batch of ArconEvent's
    #[inline]
    fn handle_events<I>(&mut self, sender: NodeID, events: I) -> ArconResult<()>
    where
        I: IntoIterator<Item = ArconEventWrapper<OP::IN>>,
    {
        'event_loop: for event in events.into_iter() {
            match event.unwrap() {
                ArconEvent::Element(e) => {
                    let watermark = match self.node_state.watermarks().get(&sender) {
                        Some(wm) => wm,
                        None => return arcon_err!("Uninitialised watermark"),
                    };

                    if e.timestamp.unwrap_or(u64::max_value()) <= watermark.timestamp {
                        continue 'event_loop;
                    }

                    unsafe {
                        (*self.operator.get()).handle_element(e, make_context!(self))?;
                    };
                }
                ArconEvent::Watermark(w) => {
                    let watermark = match self.node_state.watermarks().get(&sender) {
                        Some(wm) => wm,
                        None => return arcon_err!("Uninitialised watermark"),
                    };
                    if w <= *watermark {
                        continue 'event_loop;
                    }

                    // Insert the watermark and try early return
                    if let Some(old) = self.node_state.watermarks().insert(sender, w) {
                        if old > self.node_state.current_watermark {
                            continue 'event_loop;
                        }
                    }

                    // A different early return
                    if w <= self.node_state.current_watermark {
                        continue 'event_loop;
                    }

                    let new_watermark = *self.node_state.watermarks().values().min().unwrap();

                    if new_watermark.timestamp > self.node_state.current_watermark.timestamp {
                        self.node_state.current_watermark = new_watermark;

                        unsafe {
                            if let Some(timer) = &mut (*self.timer.get()) {
                                for timeout in timer.advance_to(new_watermark.timestamp) {
                                    (*self.operator.get())
                                        .handle_timeout(timeout, make_context!(self))?;
                                }
                            }
                        };

                        // Set current watermark
                        #[cfg(feature = "metrics")]
                        {
                            self.metrics.watermark = new_watermark;
                            self.metrics.watermark_counter.inc();
                        }

                        // Forward the watermark
                        unsafe {
                            (*self.channel_strategy.get())
                                .add(ArconEvent::Watermark(new_watermark), self);
                        };
                    }
                }
                ArconEvent::Epoch(e) => {
                    if e < self.node_state.current_epoch {
                        continue 'event_loop;
                    }

                    // Add the sender to the blocked set.
                    self.node_state.blocked_channels().insert(sender);

                    // If all senders blocked we can transition to new Epoch
                    if self.node_state.blocked_channels().len() == self.node_state.in_channels.len()
                    {
                        // persist internal node state for this node
                        self.node_state.persist()?;

                        unsafe {
                            // persist timer if enabled
                            if let Some(timer) = &mut (*self.timer.get()) {
                                timer.persist()?;
                            }
                            // persist possible operator state..
                            (*self.operator.get()).persist()?;
                        };

                        // Create checkpoint request and send it off to the NodeManager
                        let request = Arc::new(CheckpointRequest::new(self.node_state.id, e));
                        self.checkpoint_request = Some(request.clone());
                        self.node_manager_port.trigger(Checkpoint(request));

                        // Forward the Epoch
                        unsafe {
                            (*self.channel_strategy.get())
                                .add(ArconEvent::Epoch(self.node_state.current_epoch), self);
                        };

                        // Update current epoch
                        self.node_state.current_epoch = e;
                    }
                }
                ArconEvent::Death(s) => {
                    // We are instructed to shutdown....
                    unsafe {
                        (*self.channel_strategy.get()).add(ArconEvent::Death(s), self);
                    };
                    self.ctx.suicide(); // TODO: is suicide enough?
                }
            }
        }

        Ok(())
    }

    #[inline]
    fn complete_epoch(&mut self) -> ArconResult<()> {
        // flush the blocked_channels list
        self.node_state.blocked_channels().clear();

        // Clear request
        self.checkpoint_request = None;

        #[cfg(feature = "metrics")]
        {
            self.metrics.epoch = self.node_state.current_epoch;
            self.metrics.epoch_counter.inc();
        }

        // Iterate over the message-buffer until empty
        for message in self.node_state.message_buffer().consume()? {
            self.handle_events(message.sender, message.events)?;
        }

        Ok(())
    }
}

impl<OP, B> ComponentLifecycle for Node<OP, B>
where
    OP: Operator + 'static,
    B: Backend,
{
    fn on_start(&mut self) -> Handled {
        debug!(
            self.ctx.log(),
            "Started Arcon Node {} with Node ID {:?}", self.descriptor, self.node_state.id
        );

        #[cfg(feature = "metrics")]
        {
            // Start periodic timer reporting Node metrics
            if let Some(interval) = &self.ctx().config()["node_metrics_interval"].as_i64() {
                let time_dur = std::time::Duration::from_millis(*interval as u64);
                self.schedule_periodic(time_dur, time_dur, |c_self, _id| {
                    c_self.node_manager_port.trigger(NodeEvent::Metrics(
                        c_self.node_state.id,
                        c_self.metrics.clone(),
                    ));
                    Handled::Ok
                });
            }
        }

        Handled::Ok
    }
}

impl<OP, B> Require<NodeManagerPort> for Node<OP, B>
where
    OP: Operator + 'static,
    B: Backend,
{
    fn handle(&mut self, _: Never) -> Handled {
        unreachable!("Never can't be instantiated!");
    }
}

impl<OP, B> Provide<NodeManagerPort> for Node<OP, B>
where
    OP: Operator + 'static,
    B: Backend,
{
    fn handle(&mut self, e: NodeEvent) -> Handled {
        trace!(self.log(), "Ignoring node event: {:?}", e);
        Handled::Ok
    }
}

impl<OP, B> Actor for Node<OP, B>
where
    OP: Operator + 'static,
    B: Backend,
{
    type Message = ArconMessage<OP::IN>;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        if let Err(err) = self.handle_message(msg) {
            error!(self.ctx.log(), "Failed to handle message: {}", err);
        }
        Handled::Ok
    }
    fn receive_network(&mut self, msg: NetMessage) -> Handled {
        let arcon_msg = match *msg.ser_id() {
            id if id == OP::IN::RELIABLE_SER_ID => msg
                .try_deserialise::<RawArconMessage<OP::IN>, ReliableSerde<OP::IN>>()
                .map_err(|e| {
                    arcon_err_kind!("Failed to unpack reliable ArconMessage with err {:?}", e)
                }),
            #[cfg(feature = "unsafe_flight")]
            id if id == OP::IN::UNSAFE_SER_ID => msg
                .try_deserialise::<RawArconMessage<OP::IN>, UnsafeSerde<OP::IN>>()
                .map_err(|e| {
                    arcon_err_kind!("Failed to unpack unreliable ArconMessage with err {:?}", e)
                }),
            _ => {
                panic!("Unexpected deserialiser")
            }
        };

        match arcon_msg {
            Ok(m) => {
                if let Err(err) = self.handle_raw_msg(m) {
                    error!(self.ctx.log(), "Failed to handle node message: {}", err);
                }
            }
            Err(e) => error!(self.ctx.log(), "Error ArconNetworkMessage: {:?}", e),
        }
        Handled::Ok
    }
}

#[cfg(test)]
mod tests {
    // Tests the message logic of Node.
    use super::*;
    use crate::{
        pipeline::*,
        stream::{
            channel::{strategy::forward::Forward, Channel},
            node::debug::DebugNode,
            operator::function::Filter,
        },
    };
    use std::{sync::Arc, thread, time};

    fn node_test_setup() -> (ActorRef<ArconMessage<i32>>, Arc<Component<DebugNode<i32>>>) {
        // Returns a filter Node with input channels: sender1..sender3
        // And a debug sink receiving its results
        let mut pipeline = Pipeline::default();
        let pool_info = pipeline.get_pool_info();
        let epoch_manager_ref = pipeline.epoch_manager();
        let system = &pipeline.data_system();

        let sink = system.create(DebugNode::<i32>::new);

        system
            .start_notify(&sink)
            .wait_timeout(std::time::Duration::from_millis(100))
            .expect("started");

        // Construct Channel to the Debug sink
        let actor_ref: ActorRefStrong<ArconMessage<i32>> =
            sink.actor_ref().hold().expect("Failed to fetch");
        let channel = Channel::Local(actor_ref);
        let channel_strategy: ChannelStrategy<i32> =
            ChannelStrategy::Forward(Forward::new(channel, NodeID::new(0), pool_info));

        // Set up  NodeManager
        let backend = Arc::new(crate::util::temp_backend());
        let descriptor = String::from("node_");
        let in_channels = vec![1.into(), 2.into(), 3.into()];

        let nm = NodeManager::new(
            descriptor.clone(),
            epoch_manager_ref,
            in_channels.clone(),
            backend.clone(),
        );
        let node_manager_comp = system.create(|| nm);

        system
            .start_notify(&node_manager_comp)
            .wait_timeout(std::time::Duration::from_millis(100))
            .expect("started");

        fn filter_fn(x: &i32) -> bool {
            *x >= 0
        }

        let node = Node::new(
            descriptor,
            channel_strategy,
            Filter::new(&filter_fn),
            NodeState::new(NodeID::new(0), in_channels, backend),
        );

        let filter_comp = system.create(|| node);

        biconnect_components::<NodeManagerPort, _, _>(&node_manager_comp, &filter_comp)
            .expect("connection");

        system
            .start_notify(&filter_comp)
            .wait_timeout(std::time::Duration::from_millis(100))
            .expect("started");

        (filter_comp.actor_ref(), sink)
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
        sink.on_definition(|cd| {
            let data_len = cd.data.len();
            let watermark_len = cd.watermarks.len();
            assert_eq!(watermark_len, 0);
            assert_eq!(data_len, 0);
        });
    }

    #[test]
    fn node_one_watermark() {
        let (node_ref, sink) = node_test_setup();
        node_ref.tell(watermark(1, 1));
        node_ref.tell(watermark(1, 2));
        node_ref.tell(watermark(1, 3));

        wait(1);
        sink.on_definition(|cd| {
            let data_len = cd.data.len();
            let watermark_len = cd.watermarks.len();
            assert_eq!(watermark_len, 1);
            assert_eq!(data_len, 0);
        });
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
        sink.on_definition(|cd| {
            let watermark_len = cd.watermarks.len();
            assert_eq!(watermark_len, 1);
            assert_eq!(cd.watermarks[0].timestamp, 2u64);
        });
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
        sink.on_definition(|cd| {
            let data_len = cd.data.len();
            let epoch_len = cd.epochs.len();
            assert_eq!(epoch_len, 0);
            assert_eq!(cd.data[0].data, 1i32);
            assert_eq!(cd.data[1].data, 3i32);
            assert_eq!(data_len, 2);
        });
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
        sink.on_definition(|cd| {
            let data_len = cd.data.len();
            let epoch_len = cd.epochs.len();
            assert_eq!(epoch_len, 0); // no epochs should've completed
            assert_eq!(cd.data[0].data, 11i32);
            assert_eq!(cd.data[1].data, 21i32);
            assert_eq!(cd.data[2].data, 31i32);
            assert_eq!(data_len, 3);
        });
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

        wait(3);
        node_ref.tell(death(3)); // send death marker on unblocked channel to flush
        wait(3);

        sink.on_definition(|cd| {
            let data_len = cd.data.len();
            let epoch_len = cd.epochs.len();
            assert_eq!(epoch_len, 2); // 3 epochs should've completed
            assert_eq!(cd.data[0].data, 11i32);
            assert_eq!(cd.data[1].data, 21i32);
            assert_eq!(cd.data[2].data, 31i32);
            assert_eq!(cd.data[3].data, 12i32); // First message in epoch1
            assert_eq!(cd.data[4].data, 13i32); // First message in epoch2
            assert_eq!(cd.data[5].data, 22i32); // 2nd message in epoch2
            assert_eq!(data_len, 6);
        });
    }
}
