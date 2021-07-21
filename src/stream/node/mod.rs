// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

/// Debug version of [Node]
pub mod debug;

pub mod source;

use crate::conf::logger::ArconLogger;
use metrics::{
    counter, decrement_gauge, gauge, histogram, increment_counter, increment_gauge,
    register_counter, register_gauge, register_histogram, GaugeValue, Key, Recorder, Unit,
};
use perf_event::{events::Hardware, Builder, Counter, Group};
use serde::Deserialize;

#[cfg(feature = "unsafe_flight")]
use crate::data::flight_serde::unsafe_remote::UnsafeSerde;
use crate::{
    data::{flight_serde::reliable_remote::ReliableSerde, RawArconMessage, *},
    error::{ArconResult, *},
    index::{
        AppenderIndex, ArconState, EagerAppender, IndexOps, StateConstructor, Timer as ArconTimer,
    },
    manager::node::{NodeManagerEvent::Checkpoint, *},
    reportable_error,
    stream::{
        channel::strategy::ChannelStrategy,
        operator::{Operator, OperatorContext},
    },
}; // conflicts with Kompact Timer trait
use arcon_macros::ArconState;
use arcon_state::Backend;
use fxhash::*;
use kompact::prelude::*;
use std::{cell::UnsafeCell, sync::Arc};

#[cfg(feature = "metrics")]
use metrics::SetRecorderError;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusRecorder};

/// Type alias for a Node description
pub type NodeDescriptor = String;

#[cfg(feature = "hardware_counters")]
use crate::metrics::perf_event::{PerfEvents, PerformanceMetric};
use crate::metrics::runtime_metrics::{InboundThroughput, MetricValue, NodeRuntimeMetrics};

#[derive(ArconState)]
pub struct NodeState<OP: Operator + 'static, B: Backend> {
    /// Durable message buffer used for blocked channels
    message_buffer: EagerAppender<RawArconMessage<OP::IN>, B>,
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
    // perf_metrics_group: Group,
}

impl<OP: Operator + 'static, B: Backend> StateConstructor for NodeState<OP, B> {
    type BackendType = B;
    fn new(_: Arc<B>) -> Self {
        unreachable!();
    }
}

impl<OP: Operator + 'static, B: Backend> NodeState<OP, B> {
    pub fn new(id: NodeID, in_channels: Vec<NodeID>, backend: Arc<B>) -> Self {
        let message_buffer = EagerAppender::new("_messagebuffer", backend);

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
            &$sel.logger,
            &$sel.descriptor,
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

    /// Internal Node State
    node_state: NodeState<OP, B>,
    /// Event time scheduler
    timer: UnsafeCell<ArconTimer<u64, OP::TimerState, B>>,
    logger: ArconLogger,

    #[cfg(feature = "hardware_counters")]
    performance_metric: PerformanceMetric,
    node_runtime_metrics: NodeRuntimeMetrics,
}

impl<OP, B> Node<OP, B>
where
    OP: Operator + 'static,
    B: Backend,
{
    /// Creates a new Node
    pub(crate) fn new(
        descriptor: NodeDescriptor,
        channel_strategy: ChannelStrategy<OP::OUT>,
        operator: OP,
        node_state: NodeState<OP, B>,
        backend: Arc<B>,
        logger: ArconLogger,

        #[cfg(feature = "hardware_counters")] perf_events: PerfEvents,
    ) -> Self {
        let timer_id = format!("_{}_timer", descriptor);
        let timer = ArconTimer::new(timer_id, backend);
        let borrowed_descriptor: &str = &descriptor.clone();

        #[cfg(feature = "hardware_counters")]
        let performance_metric = {
            let mut performance_metrics_group = Group::new().unwrap();
            let mut performance_metric = PerformanceMetric {
                performance_metrics_group,
                hardware_metric_counters: vec![],
            };

            let iterator = perf_events.hardware_metric_kind_vector.iter();
            for val in iterator {
                performance_metric.hardware_metric_counters.push((
                    String::from(val.get_counter_as_string()),
                    Builder::new()
                        .group(&mut performance_metric.performance_metrics_group)
                        .kind(val.get_hardware_kind())
                        .build()
                        .unwrap(),
                ));
            }
            performance_metric.register_performance_metric_gauges(descriptor.clone(), perf_events);
            performance_metric
        };

        Node {
            ctx: ComponentContext::uninitialised(),
            node_manager_port: RequiredPort::uninitialised(),
            descriptor,
            channel_strategy: UnsafeCell::new(channel_strategy),
            operator: UnsafeCell::new(operator),

            node_state,
            timer: UnsafeCell::new(timer),
            logger,

            #[cfg(feature = "hardware_counters")]
            performance_metric,

            node_runtime_metrics: NodeRuntimeMetrics::new(borrowed_descriptor),
        }
    }

    /// Message handler for both locally and remote sent messages
    #[inline]
    fn handle_message(&mut self, message: MessageContainer<OP::IN>) -> ArconResult<()> {
        self.node_runtime_metrics
            .inbound_throughput
            .update_value(message.total_events());
        if !self.node_state.in_channels.contains(message.sender()) {
            error!(
                self.logger,
                "Message from invalid sender id {:?}",
                message.sender()
            );
            return Ok(());
        }

        if self.sender_blocked(message.sender()) {
            self.node_state.message_buffer().append(message.raw())?;
            return Ok(());
        }
        match message {
            MessageContainer::Raw(r) => self.handle_events(r.sender, r.events)?,
            MessageContainer::Local(l) => self.handle_events(l.sender, l.events)?,
        }
        gauge!(
            [&self.descriptor, "_inbound_throughput"].join("\n"),
            self.node_runtime_metrics.inbound_throughput.get_value()
        );

        #[cfg(feature = "hardware_counters")]
        {
            let counts = self.performance_metric.performance_metrics_group.read()?;
            let counter_iterator = self.performance_metric.hardware_metric_counters.iter();
            for (metric_name, counter) in counter_iterator {
                gauge!(
                    self.performance_metric
                        .get_field_gauge_name(metric_name, &self.descriptor),
                    counts[&counter] as f64
                );
            }
        }

        Ok(())
    }

    #[inline(always)]
    fn sender_blocked(&mut self, sender: &NodeID) -> bool {
        self.node_state.blocked_channels().contains(sender)
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
                        None => return reportable_error!("Uninitialised watermark"),
                    };

                    if e.timestamp.unwrap_or(u64::max_value()) <= watermark.timestamp {
                        continue 'event_loop;
                    }

                    unsafe {
                        let operator = &mut (*self.operator.get());
                        // Set key for the current element
                        // TODO: Should use a pre-defined key for Non-Keyed Streams.
                        operator.state().set_key(e.data.get_key());
                        operator.handle_element(e, make_context!(self))?;
                    };
                }
                ArconEvent::Watermark(w) => {
                    let watermark = match self.node_state.watermarks().get(&sender) {
                        Some(wm) => wm,
                        None => return reportable_error!("Uninitialised watermark"),
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
                            let timer = &mut (*self.timer.get());
                            let timeouts = timer.advance_to(new_watermark.timestamp)?;
                            for timeout in timeouts {
                                (*self.operator.get())
                                    .handle_timeout(timeout, make_context!(self))?;
                            }
                        };
                        self.node_runtime_metrics.watermark_counter.update_value(1);
                        gauge!(
                            [&self.descriptor, "_watermark_counter"].join("\n"),
                            self.node_runtime_metrics.watermark_counter.get_value()
                        );

                        // Forward the watermark
                        unsafe {
                            (*self.channel_strategy.get())
                                .add(ArconEvent::Watermark(new_watermark), self);
                        };
                    }
                }
                ArconEvent::Epoch(e) => {
                    debug!(self.logger, "Got Epoch {:?}", e);
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
                            // persist timer
                            let timer = &mut (*self.timer.get());
                            timer.persist()?;

                            // persist possible operator state..
                            (*self.operator.get()).persist()?;
                        };

                        // Create checkpoint request and send it off to the NodeManager
                        let request = CheckpointRequest::new(
                            self.node_state.id,
                            self.node_state.current_epoch,
                        );
                        self.node_manager_port.trigger(Checkpoint(request));

                        // Forward the Epoch
                        unsafe {
                            (*self.channel_strategy.get())
                                .add(ArconEvent::Epoch(self.node_state.current_epoch), self);
                        };

                        // Update current epoch
                        self.node_state.current_epoch.epoch += 1;
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
        self.node_runtime_metrics.epoch_counter.update_value(1);
        increment_gauge!(
            [&self.descriptor, "_epoch_counter"].join("\n"),
            self.node_runtime_metrics.epoch_counter.get_value()
        );
        // flush the blocked_channels list
        self.node_state.blocked_channels().clear();

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
            self.logger,
            "Started Arcon Node {} with Node ID {:?}", self.descriptor, self.node_state.id
        );

        unsafe {
            let operator = &mut (*self.operator.get());
            if operator.on_start(make_context!(self)).is_err() {
                error!(self.logger, "Failed to run startup code");
            }
        };

        Handled::Ok
    }
}

impl<OP, B> Require<NodeManagerPort> for Node<OP, B>
where
    OP: Operator + 'static,
    B: Backend,
{
    fn handle(&mut self, event: NodeEvent) -> Handled {
        match event {
            NodeEvent::CheckpointResponse(_) => {
                if let Err(error) = self.complete_epoch() {
                    error!(
                        self.logger,
                        "Failed to complete epoch with error {:?}", error
                    );
                }
            }
        }
        Handled::Ok
    }
}

impl<OP, B> Provide<NodeManagerPort> for Node<OP, B>
where
    OP: Operator + 'static,
    B: Backend,
{
    fn handle(&mut self, e: NodeManagerEvent) -> Handled {
        trace!(self.logger, "Ignoring node event: {:?}", e);
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
        if let Err(err) = self.handle_message(MessageContainer::Local(msg)) {
            error!(self.logger, "Failed to handle message: {}", err);
        }
        Handled::Ok
    }
    fn receive_network(&mut self, msg: NetMessage) -> Handled {
        let arcon_msg = match *msg.ser_id() {
            id if id == OP::IN::RELIABLE_SER_ID => msg
                .try_deserialise::<RawArconMessage<OP::IN>, ReliableSerde<OP::IN>>()
                .map_err(|e| Error::Unsupported {
                    msg: format!("Failed to unpack reliable ArconMessage with err {:?}", e),
                }),
            #[cfg(feature = "unsafe_flight")]
            id if id == OP::IN::UNSAFE_SER_ID => msg
                .try_deserialise::<RawArconMessage<OP::IN>, UnsafeSerde<OP::IN>>()
                .map_err(|e| Error::Unsupported {
                    msg: format!("Failed to unpack unreliable ArconMessage with err {:?}", e),
                }),
            id => reportable_error!("Unexpected deserialiser with id {}", id),
        };

        match arcon_msg {
            Ok(m) => {
                if let Err(err) = self.handle_message(MessageContainer::Raw(m)) {
                    error!(self.logger, "Failed to handle node message: {}", err);
                }
            }
            Err(e) => error!(self.logger, "Error ArconNetworkMessage: {:?}", e),
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
        fn filter_fn(x: &i32) -> bool {
            *x >= 0
        }

        let filter = Filter::new(&filter_fn);

        fn setup<OP: Operator<IN = i32, OUT = i32> + 'static>(
            op: OP,
        ) -> (ActorRef<ArconMessage<i32>>, Arc<Component<DebugNode<i32>>>) {
            // Returns a filter Node with input channels: sender1..sender3
            // And a debug sink receiving its results
            let mut pipeline = Pipeline::default();
            let pool_info = pipeline.get_pool_info();
            let epoch_manager_ref = pipeline.epoch_manager();

            let sink = pipeline.data_system().create(DebugNode::<i32>::new);

            pipeline
                .data_system()
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
            let backend = Arc::new(crate::test_utils::temp_backend());
            let descriptor = String::from("node_");
            let in_channels = vec![1.into(), 2.into(), 3.into()];

            let nm = NodeManager::<OP, _>::new(
                descriptor.clone(),
                pipeline.data_system.clone(),
                epoch_manager_ref,
                in_channels.clone(),
                backend.clone(),
                pipeline.arcon_logger.clone(),
            );
            let node_manager_comp = pipeline.ctrl_system().create(|| nm);

            pipeline
                .ctrl_system()
                .start_notify(&node_manager_comp)
                .wait_timeout(std::time::Duration::from_millis(100))
                .expect("started");

            let node = Node::<OP, _>::new(
                descriptor,
                channel_strategy,
                op,
                NodeState::new(NodeID::new(0), in_channels, backend.clone()),
                backend,
                pipeline.arcon_logger.clone(),
            );

            let filter_comp = pipeline.data_system().create(|| node);
            let required_ref = filter_comp.on_definition(|cd| cd.node_manager_port.share());

            biconnect_components::<NodeManagerPort, _, _>(&node_manager_comp, &filter_comp)
                .expect("connection");

            pipeline
                .data_system()
                .start_notify(&filter_comp)
                .wait_timeout(std::time::Duration::from_millis(100))
                .expect("started");

            let filter_ref = filter_comp.actor_ref();

            node_manager_comp.on_definition(|cd| {
                // Insert the created Node into the NodeManager
                cd.nodes.insert(NodeID::new(0), (filter_comp, required_ref));
            });

            (filter_ref, sink)
        }

        setup(filter)
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
