// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

/// Debug version of [Node]
pub mod debug;

use crate::{
    data::RawArconMessage,
    manager::node_manager::*,
    metrics::{counter::Counter, gauge::Gauge, meter::Meter},
    prelude::{
        state::{Bundle, Handle, MapState, ValueState, VecState},
        *,
    },
    stream::operator::OperatorContext,
    timer::TimerBackend,
};
use std::{cell::RefCell, iter};

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

// `arcon_state` instead of `state` just because of a bug in IntelliJ Rust related to macro
// resolution
arcon_state::bundle! {
    /// Internal Node State
    pub struct NodeState<OpIn: ArconType> {
        /// Mappings from NodeID to current Watermark
        watermarks: Handle<MapState<NodeID, Watermark>>,
        /// Current Watermark value
        current_watermark: Handle<ValueState<Watermark>>,
        /// Current Epoch
        current_epoch: Handle<ValueState<Epoch>>,
        /// Blocked channels during epoch alignment
        blocked_channels: Handle<MapState<NodeID, ()>>,
        /// Temporary message buffer used while having blocked channels
        message_buffer: Handle<VecState<RawArconMessage<OpIn>>>,
    }
}

impl<OpIn: ArconType> NodeState<OpIn> {
    fn new() -> NodeState<OpIn> {
        NodeState {
            watermarks: Handle::map("__node_watermarks"),
            current_watermark: Handle::value("__node_current_watermark"),
            current_epoch: Handle::value("__node_current_epoch"),
            blocked_channels: Handle::map("__node_blocked_channels"),
            message_buffer: Handle::vec("__node_message_buffer"),
        }
    }
}

/// A Node is a [kompact] component that drives the execution of streaming operators
///
/// Nodes receive [ArconMessage] and run some transform on the data
/// before using a [ChannelStrategy] to send the result to another Node.
#[derive(ComponentDefinition)]
pub struct Node<OP, B, T>
// TODO: remove the default state backend
where
    OP: Operator<B> + 'static,
    B: state::Backend,
    T: TimerBackend<OP::TimerState>,
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
    channel_strategy: RefCell<ChannelStrategy<OP::OUT>>,
    /// Current set of IDs connected to this Node
    in_channels: Vec<NodeID>,
    /// User-defined Operator
    operator: OP,
    /// Internal state of the Node
    state: NodeState<OP::IN>,
    /// Metrics collected by the Node
    metrics: RefCell<NodeMetrics>,
    /// State Backend used to persist data
    pub state_backend: state::BackendContainer<B>,
    /// Timer Backend to keep track of event timers
    timer_backend: RefCell<T>,
}

// Just a shorthand to avoid repeating the OperatorContext construction everywhere
macro_rules! make_context {
    ($sel:ident, $sess:ident) => {
        OperatorContext::new(
            &mut *$sel.channel_strategy.borrow_mut(),
            $sess,
            &mut *$sel.timer_backend.borrow_mut(),
        )
    };
}

impl<OP, B, T> Node<OP, B, T>
where
    OP: Operator<B> + 'static,
    B: state::Backend,
    T: TimerBackend<OP::TimerState>,
{
    /// Creates a new Node
    pub fn new(
        descriptor: NodeDescriptor,
        id: NodeID,
        in_channels: Vec<NodeID>,
        channel_strategy: ChannelStrategy<OP::OUT>,
        mut operator: OP,
        state_backend: state::BackendContainer<B>,
        mut timer_backend: T,
    ) -> Self {
        let mut sb_session = state_backend.session();

        let mut state = NodeState::<OP::IN>::new();

        // register all the states that will ever be used by this Node
        {
            // SAFETY: we specifically want this to be the only place that is supposed to call this
            let mut registration_token = unsafe { state::RegistrationToken::new(&mut sb_session) };

            state.register_states(&mut registration_token);
            operator.register_states(&mut registration_token);
            timer_backend.register_states(&mut registration_token);
        }

        // initialize timer state
        timer_backend.init(&mut sb_session);

        // initialize internal node state
        {
            let mut active_state = state.activate(&mut sb_session);
            for channel in &in_channels {
                let mut watermarks = active_state.watermarks();
                if !watermarks
                    .contains(channel)
                    .expect("Could not check watermarks")
                {
                    watermarks
                        .fast_insert(*channel, Watermark { timestamp: 0 })
                        .expect("Could not initialize watermarks");
                }
            }

            if active_state
                .current_watermark()
                .get()
                .expect("watermark get error")
                .is_none()
            {
                active_state
                    .current_watermark()
                    .set(Watermark { timestamp: 0 })
                    .expect("watermark set error");
            }

            if active_state
                .current_epoch()
                .get()
                .expect("current epoch get error")
                .is_none()
            {
                active_state
                    .current_epoch()
                    .set(Epoch { epoch: 0 })
                    .expect("current epoch set error");
            }
        }

        // initialize operator state
        operator.init(&mut sb_session);

        // Set up metrics structure
        let mut metrics = NodeMetrics::new();
        metrics.inbound_channels.inc_n(in_channels.len());
        metrics
            .outbound_channels
            .inc_n(channel_strategy.num_channels());

        let metrics = RefCell::new(metrics);
        let timer_backend = RefCell::new(timer_backend);
        let channel_strategy = RefCell::new(channel_strategy);

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

    /// Handle a Raw ArconMessage that has either been sent remotely or temporarily stored in the state backend
    #[inline]
    fn handle_raw_msg(&mut self, message: RawArconMessage<OP::IN>) -> ArconResult<()> {
        // Check valid sender
        if !self.in_channels.contains(&message.sender) {
            return arcon_err!("Message from invalid sender");
        }

        self.record_incoming_events(message.events.len() as u64);

        let mut sb_session = self.state_backend.session();

        {
            let mut active_state = self.state.activate(&mut sb_session);
            // Check if sender is blocked
            if active_state.blocked_channels().contains(&message.sender)? {
                // Add the message to the back of the queue
                active_state.message_buffer().append(message)?;
                return Ok(());
            }
        }

        // If sender is not blocked, process events.
        let res = self.handle_events(message.sender, message.events, &mut sb_session);
        drop(sb_session);
        res
    }

    /// Handle a local ArconMessage that is backed by the [ArconAllocator]
    #[inline]
    fn handle_message(&mut self, message: ArconMessage<OP::IN>) -> ArconResult<()> {
        // Check valid sender
        if !self.in_channels.contains(&message.sender) {
            return arcon_err!("Message from invalid sender");
        }

        self.record_incoming_events(message.events.len() as u64);

        let mut sb_session = self.state_backend.session();

        let mut state = self.state.activate(&mut sb_session);
        // Check if sender is blocked
        if state.blocked_channels().contains(&message.sender)? {
            // Add the message to the back of the queue
            state.message_buffer().append(message.into())?;
            return Ok(());
        }

        // If sender is not blocked, process events.
        let res = self.handle_events(message.sender, message.events, &mut sb_session);
        drop(sb_session);
        res
    }

    /// Mark amount of inbound events
    #[inline(always)]
    fn record_incoming_events(&self, total: u64) {
        self.metrics.borrow_mut().inbound_throughput.mark_n(total);
    }

    /// Iterate over a batch of ArconEvent's
    #[inline]
    fn handle_events<I>(
        &self,
        sender: NodeID,
        events: I,
        sb_session: &mut state::Session<B>,
    ) -> ArconResult<()>
    where
        I: IntoIterator<Item = ArconEventWrapper<OP::IN>>,
    {
        'event_loop: for event in events.into_iter() {
            let mut state = self.state.activate(sb_session);
            match event.unwrap() {
                ArconEvent::Element(e) => {
                    if e.timestamp.unwrap_or(u64::max_value())
                        <= state
                            .watermarks()
                            .get(&sender)?
                            .ok_or_else(|| arcon_err_kind!("uninitialized watermark"))?
                            .timestamp
                    {
                        continue 'event_loop;
                    }

                    drop(state);
                    self.operator
                        .handle_element(e, make_context!(self, sb_session));
                }
                ArconEvent::Watermark(w) => {
                    if w <= state
                        .watermarks()
                        .get(&sender)?
                        .ok_or_else(|| arcon_err_kind!("uninitialized watermark"))?
                    {
                        continue 'event_loop;
                    }

                    let current_watermark = state
                        .current_watermark()
                        .get()?
                        .ok_or_else(|| arcon_err_kind!("current watermark uninitialized"))?;

                    // Insert the watermark and try early return
                    if let Some(old) = state.watermarks().insert(sender, w)? {
                        if old > current_watermark {
                            continue 'event_loop;
                        }
                    }
                    // A different early return
                    if w <= current_watermark {
                        continue 'event_loop;
                    }

                    // Let new_watermark take the value of the lowest watermark
                    let new_watermark = state
                        .watermarks()
                        .values()?
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
                        state.current_watermark().set(new_watermark)?;
                        drop(state);
                        // Handle the watermark
                        self.operator
                            .handle_watermark(new_watermark, make_context!(self, sb_session));

                        for timeout in self
                            .timer_backend
                            .borrow_mut()
                            .advance_to(new_watermark.timestamp, sb_session)
                        {
                            self.operator
                                .handle_timeout(timeout, make_context!(self, sb_session));
                        }

                        let mut metrics = self.metrics.borrow_mut();

                        // Set current watermark
                        metrics.watermark = new_watermark;

                        // Forward the watermark
                        self.channel_strategy
                            .borrow_mut()
                            .add(ArconEvent::Watermark(new_watermark));

                        // increment watermark counter
                        metrics.watermark_counter.inc();
                    }
                }
                ArconEvent::Epoch(e) => {
                    if e <= state
                        .current_epoch()
                        .get()?
                        .ok_or_else(|| arcon_err_kind!("uninitialized epoch"))?
                    {
                        continue 'event_loop;
                    }

                    // Add the sender to the blocked set.
                    state.blocked_channels().fast_insert(sender, ())?;

                    // If all senders blocked we can transition to new Epoch
                    if state.blocked_channels().len()? == self.in_channels.len() {
                        // update current epoch
                        state.current_epoch().set(e)?;
                        drop(state);

                        self.operator
                            .handle_epoch(e, make_context!(self, sb_session));
                        self.timer_backend.borrow_mut().handle_epoch(e, sb_session);

                        // store the state
                        self.save_state(sb_session)?;

                        let mut metrics = self.metrics.borrow_mut();
                        // Set current epoch
                        metrics.epoch = e;

                        // forward the epoch
                        self.channel_strategy.borrow_mut().add(ArconEvent::Epoch(e));

                        // increment epoch counter
                        metrics.epoch_counter.inc();

                        self.after_state_save(sb_session)?;
                    }
                }
                ArconEvent::Death(s) => {
                    // We are instructed to shutdown....
                    self.channel_strategy.borrow_mut().add(ArconEvent::Death(s));
                    // TODO: invoke shutdown operations...
                }
            }
        }
        Ok(())
    }

    fn save_state(&self, sb_session: &mut state::Session<B>) -> ArconResult<()> {
        let mut state = self.state.activate(sb_session);
        if let Some(base_dir) = &self.ctx.config()["checkpoint_dir"].as_string() {
            let checkpoint_dir = format!(
                "{}/checkpoint_{id}_{epoch}",
                base_dir,
                id = self.id.id,
                epoch = state
                    .current_epoch()
                    .get()?
                    .ok_or_else(|| arcon_err_kind!("current epoch uninitialized"))?
                    .epoch
            );
            sb_session.backend.checkpoint(checkpoint_dir.as_ref())?;
            debug!(
                self.ctx.log(),
                "Completed a Checkpoint to path {}", checkpoint_dir
            );
        } else {
            return arcon_err!("Failed to fetch checkpoint_dir from Config");
        }

        Ok(())
    }

    fn after_state_save(&self, sb_session: &mut state::Session<B>) -> ArconResult<()> {
        let mut state = self.state.activate(sb_session);
        // flush the blocked_channels list
        state.blocked_channels().clear()?;

        let mut message_buffer = state.message_buffer();

        // Handle the message buffer.
        if !message_buffer.is_empty()? {
            // Get a local copy of the buffer
            let local_buffer = message_buffer.get()?;
            message_buffer.clear()?;

            // Iterate over the message-buffer until empty
            for message in local_buffer {
                self.handle_events(message.sender, message.events, sb_session)?;
            }
        }

        Ok(())
    }
}

impl<OP, B, T> Provide<ControlPort> for Node<OP, B, T>
where
    OP: Operator<B> + 'static,
    B: state::Backend,
    T: TimerBackend<OP::TimerState>,
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
                        c_self.node_manager_port.trigger(NodeEvent::Metrics(
                            c_self.id,
                            c_self.metrics.borrow().clone(),
                        ));
                    });
                }

                if self.state_backend.get_mut().was_restored() {
                    if let Err(e) = self.after_state_save(&mut self.state_backend.session()) {
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

impl<OP, B, T> Require<NodeManagerPort> for Node<OP, B, T>
where
    OP: Operator<B> + 'static,
    B: state::Backend,
    T: TimerBackend<OP::TimerState>,
{
    fn handle(&mut self, _: Never) {
        unreachable!("Never can't be instantiated!");
    }
}
impl<OP, B, T> Provide<NodeManagerPort> for Node<OP, B, T>
where
    OP: Operator<B> + 'static,
    B: state::Backend,
    T: TimerBackend<OP::TimerState>,
{
    fn handle(&mut self, e: NodeEvent) {
        trace!(self.log(), "Ignoring node event: {:?}", e)
    }
}

impl<OP, B, T> Actor for Node<OP, B, T>
where
    OP: Operator<B> + 'static,
    B: state::Backend,
    T: TimerBackend<OP::TimerState>,
{
    type Message = ArconMessage<OP::IN>;

    fn receive_local(&mut self, msg: Self::Message) {
        if let Err(err) = self.handle_message(msg) {
            error!(self.ctx.log(), "Failed to handle message: {}", err);
        }
    }
    fn receive_network(&mut self, msg: NetMessage) {
        let unsafe_id = OP::IN::UNSAFE_SER_ID;
        let reliable_id = OP::IN::RELIABLE_SER_ID;
        let ser_id = *msg.ser_id();

        let arcon_msg = {
            if ser_id == reliable_id {
                msg.try_deserialise::<RawArconMessage<OP::IN>, ReliableSerde<OP::IN>>()
                    .map_err(|e| {
                        arcon_err_kind!("Failed to unpack reliable ArconMessage with err {:?}", e)
                    })
            } else if ser_id == unsafe_id {
                msg.try_deserialise::<RawArconMessage<OP::IN>, UnsafeSerde<OP::IN>>()
                    .map_err(|e| {
                        arcon_err_kind!("Failed to unpack unreliable ArconMessage with err {:?}", e)
                    })
            } else {
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
        let pool_info = pipeline.get_pool_info();
        let system = &pipeline.system();

        let sink = system.create(move || DebugNode::<i32>::new());
        system.start(&sink);
        let actor_ref: ActorRefStrong<ArconMessage<i32>> =
            sink.actor_ref().hold().expect("Failed to fetch");
        let channel = Channel::Local(actor_ref);
        let channel_strategy: ChannelStrategy<i32> =
            ChannelStrategy::Forward(Forward::new(channel, NodeID::new(0), pool_info));

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

        node_ref.tell(death(3)); // send death marker on unblocked channel to flush
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

        node_ref.tell(death(3)); // send death marker on unblocked channel to flush
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
