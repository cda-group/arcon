// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{Window, WindowContext};
use crate::{
    data::{ArconElement, ArconType},
    index::{ArconState, EagerHashTable, IndexOps, StateConstructor},
    stream::operator::{Operator, OperatorContext},
};
use arcon_error::*;
use arcon_macros::ArconState;
use arcon_state::Backend;
use kompact::prelude::ComponentDefinition;
use prost::Message;
use std::{marker::PhantomData, sync::Arc};

type Key = u64;
type Index = u64;
type Timestamp = u64;

#[cfg_attr(feature = "arcon_serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Message, PartialEq, Clone)]
pub struct WindowEvent {
    #[prost(uint64, tag = "1")]
    key: Key,
    #[prost(uint64, tag = "2")]
    index: Index,
    #[prost(uint64, tag = "3")]
    timestamp: Timestamp,
}

impl WindowEvent {
    fn new(key: Key, index: Index, timestamp: Timestamp) -> WindowEvent {
        WindowEvent {
            key,
            index,
            timestamp,
        }
    }
}

#[derive(ArconState)]
pub struct AssignerState<B: Backend> {
    window_start: EagerHashTable<Key, Timestamp, B>,
    active_windows: EagerHashTable<WindowContext, (), B>,
}

impl<B: Backend> StateConstructor for AssignerState<B> {
    type BackendType = B;
    fn new(backend: Arc<Self::BackendType>) -> Self {
        Self {
            window_start: EagerHashTable::new("_window_start", backend.clone()),
            active_windows: EagerHashTable::new("_active_windows", backend),
        }
    }
}

/// Window Assigner Based on Event Time
///
/// IN: Input event
/// OUT: Output of Window
pub struct WindowAssigner<IN, OUT, W, B>
where
    IN: ArconType,
    OUT: ArconType,
    W: Window<IN, OUT>,
    B: Backend,
{
    // effectively immutable, so no reason to persist
    window_length: u64,
    window_slide: u64,
    late_arrival_time: u64,
    keyed: bool,

    // window keeps its own state per key and index (via state backend api)
    window: W,
    // simply persisted state
    state: AssignerState<B>,
    op_state: (),
    _marker: PhantomData<(IN, OUT)>,
}

impl<IN, OUT, W, B> WindowAssigner<IN, OUT, W, B>
where
    IN: ArconType,
    OUT: ArconType,
    W: Window<IN, OUT>,
    B: Backend,
{
    /// Create a WindowAssigner for tumbling windows
    pub fn tumbling(
        window: W,
        backend: Arc<B>,
        length: u64,
        late_arrival_time: u64,
        keyed: bool,
    ) -> Self {
        let slide = length; // slide = length means that we operate on tumbling windows
        Self::setup(window, backend, length, slide, late_arrival_time, keyed)
    }

    /// Create a WindowAssigner for sliding windows
    pub fn sliding(
        window: W,
        backend: Arc<B>,
        length: u64,
        slide: u64,
        late_arrival_time: u64,
        keyed: bool,
    ) -> Self {
        Self::setup(window, backend, length, slide, late_arrival_time, keyed)
    }

    // Setup method for both sliding and tumbling windows
    fn setup(window: W, backend: Arc<B>, length: u64, slide: u64, late: u64, keyed: bool) -> Self {
        // Sanity check on slide and length
        if length < slide {
            panic!("Window Length lower than slide!");
        }
        if length % slide != 0 {
            panic!("Window Length not divisible by slide!");
        }

        let state = AssignerState::new(backend);

        WindowAssigner {
            window_length: length,
            window_slide: slide,
            late_arrival_time: late,
            window,
            keyed,

            state,
            op_state: (),
            _marker: Default::default(),
        }
    }

    #[inline]
    fn new_window_trigger(
        &mut self,
        window_ctx: WindowContext,
        ctx: &mut OperatorContext<Self, impl Backend, impl ComponentDefinition>,
    ) -> ArconResult<()> {
        let window_start = match self.state.window_start().get(&window_ctx.key)? {
            Some(start) => start,
            None => {
                return arcon_err!(
                    "Unexpected failure, could not find window start for existing key"
                )
            }
        };

        let ts = window_start + (window_ctx.index * self.window_slide) + self.window_length;

        ctx.schedule_at(
            window_ctx,
            ts + self.late_arrival_time,
            WindowEvent::new(window_ctx.key, window_ctx.index, ts),
        )
        .map_err(|_| arcon_err_kind!("Attempted to schedule an expired timer"))
    }

    #[inline]
    fn get_key(&self, e: &ArconElement<IN>) -> u64 {
        if !self.keyed {
            return 0;
        }
        e.data.get_key()
    }
}

impl<IN, OUT, W, B> Operator for WindowAssigner<IN, OUT, W, B>
where
    IN: ArconType,
    OUT: ArconType,
    W: Window<IN, OUT>,
    B: Backend,
{
    type IN = IN;
    type OUT = OUT;
    type TimerState = WindowEvent;
    type OperatorState = ();

    fn handle_element(
        &mut self,
        element: ArconElement<IN>,
        mut ctx: OperatorContext<Self, impl Backend, impl ComponentDefinition>,
    ) -> OperatorResult<()> {
        let ts = element.timestamp.unwrap_or(1);

        let time = ctx.current_time()?;

        let ts_lower_bound = time.saturating_sub(self.late_arrival_time);

        if ts < ts_lower_bound {
            // Late arrival: early return
            return Ok(());
        }

        let key = self.get_key(&element);

        // Will store the index of the highest and lowest window it should go into
        let mut floor = 0;
        let mut ceil = 0;

        match self.state.window_start().get(&key)? {
            Some(start) => {
                // Get the highest window the element goes into
                ceil = (ts - start) / self.window_slide;
                if ceil >= (self.window_length / self.window_slide) {
                    floor = ceil - (self.window_length / self.window_slide) + 1;
                }
            }
            None => {
                self.state.window_start().put(key, ts)?;
            }
        }

        // For all windows, insert element....
        for index in floor..=ceil {
            let window_ctx = WindowContext { key, index };
            self.window.on_element(element.data.clone(), window_ctx)?;

            let active_exist = self.state.active_windows().contains(&window_ctx)?;

            // if it does not exist, then add active window and create trigger
            if !active_exist {
                self.state.active_windows().put(window_ctx, ())?;

                if let Err(event) = self.new_window_trigger(window_ctx, &mut ctx) {
                    // I'm pretty sure this shouldn't happen
                    unreachable!("Window was expired when scheduled: {:?}", event);
                }
            }
        }

        Ok(())
    }

    fn handle_timeout(
        &mut self,
        timeout: Self::TimerState,
        mut ctx: OperatorContext<Self, impl Backend, impl ComponentDefinition>,
    ) -> OperatorResult<()> {
        let WindowEvent {
            key,
            index,
            timestamp,
        } = timeout;

        let window_ctx = WindowContext::new(key, index);

        let result = self.window.result(window_ctx)?;

        self.window.clear(window_ctx)?;
        self.state.active_windows().remove(&window_ctx)?;

        ctx.output(ArconElement::with_timestamp(result, timestamp));
        Ok(())
    }

    fn persist(&mut self) -> OperatorResult<()> {
        self.state.persist()
    }
    fn state(&mut self) -> &mut Self::OperatorState {
        &mut self.op_state
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data::{ArconMessage, NodeID},
        manager::node::{NodeManager, NodeManagerPort},
        pipeline::*,
        stream::{
            channel::{
                strategy::{forward::Forward, ChannelStrategy},
                Channel,
            },
            node::{debug::DebugNode, Node, NodeState},
            operator::window::AppenderWindow,
        },
    };
    use kompact::prelude::{biconnect_components, ActorRefFactory, ActorRefStrong, Component};
    use std::{sync::Arc, thread, time, time::UNIX_EPOCH};

    // helper functions
    fn window_assigner_test_setup(
        length: u64,
        slide: u64,
        late: u64,
    ) -> (
        ActorRefStrong<ArconMessage<u64>>,
        Arc<Component<DebugNode<u64>>>,
    ) {
        let mut pipeline = Pipeline::default();
        let pool_info = pipeline.get_pool_info();
        let epoch_manager_ref = pipeline.epoch_manager();

        // Create a sink
        let sink = pipeline.data_system().create(DebugNode::<u64>::new);

        pipeline
            .data_system()
            .start_notify(&sink)
            .wait_timeout(std::time::Duration::from_millis(100))
            .expect("started");

        let sink_ref: ActorRefStrong<ArconMessage<u64>> =
            sink.actor_ref().hold().expect("failed to get strong ref");

        let channel_strategy = ChannelStrategy::Forward(Forward::new(
            Channel::Local(sink_ref),
            NodeID::new(1),
            pool_info,
        ));

        let backend = Arc::new(crate::test_utils::temp_backend());
        let descriptor = String::from("node_");
        let in_channels = vec![0.into()];

        let nm = NodeManager::<
            WindowAssigner<
                u64,
                u64,
                AppenderWindow<u64, u64, arcon_state::Sled>,
                arcon_state::Sled,
            >,
            _,
        >::new(
            descriptor.clone(),
            pipeline.data_system.clone(),
            epoch_manager_ref,
            in_channels.clone(),
            backend.clone(),
        );

        let node_manager_comp = pipeline.ctrl_system().create(|| nm);

        pipeline
            .ctrl_system()
            .start_notify(&node_manager_comp)
            .wait_timeout(std::time::Duration::from_millis(100))
            .expect("started");

        fn appender_fn(u: &[u64]) -> u64 {
            u.len() as u64
        }

        let window = AppenderWindow::new(backend.clone(), &appender_fn);

        let window_assigner =
            WindowAssigner::sliding(window, backend.clone(), length, slide, late, true);

        let node = Node::new(
            descriptor,
            channel_strategy,
            window_assigner,
            NodeState::new(NodeID::new(0), in_channels, backend.clone()),
            backend,
        );

        let window_comp = pipeline.data_system().create(|| node);
        let required_ref = window_comp.on_definition(|cd| cd.node_manager_port.share());

        biconnect_components::<NodeManagerPort, _, _>(&node_manager_comp, &window_comp)
            .expect("connection");

        pipeline
            .data_system()
            .start_notify(&window_comp)
            .wait_timeout(std::time::Duration::from_millis(100))
            .expect("started");

        let win_ref: ActorRefStrong<ArconMessage<u64>> = window_comp
            .actor_ref()
            .hold()
            .expect("failed to get strong ref");

        node_manager_comp.on_definition(|cd| {
            // Insert the created Node into the NodeManager
            cd.nodes.insert(NodeID::new(0), (window_comp, required_ref));
        });

        (win_ref, sink)
    }
    fn now() -> u64 {
        time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("error")
            .as_secs()
    }
    fn wait(time: u64) {
        thread::sleep(time::Duration::from_secs(time));
    }
    fn watermark(time: u64) -> ArconMessage<u64> {
        ArconMessage::watermark(time, 0.into())
    }
    fn timestamped_event(ts: u64) -> ArconMessage<u64> {
        ArconMessage::element(1u64, Some(ts), 0.into())
    }
    fn timestamped_keyed_event(ts: u64, id: u64) -> ArconMessage<u64> {
        ArconMessage::element(id, Some(ts), 0.into())
    }

    // Tests:
    #[test]
    fn window_by_key() {
        let (assigner_ref, sink) = window_assigner_test_setup(10, 5, 0);
        wait(1);
        let moment = now();
        assigner_ref.tell(timestamped_keyed_event(moment, 1));
        assigner_ref.tell(timestamped_keyed_event(moment + 1, 2));
        assigner_ref.tell(timestamped_keyed_event(moment + 2, 3));
        assigner_ref.tell(timestamped_keyed_event(moment + 3, 2));
        assigner_ref.tell(timestamped_keyed_event(moment + 5, 2));
        assigner_ref.tell(timestamped_keyed_event(moment + 4, 1));

        wait(1);
        assigner_ref.tell(watermark(moment + 12));
        wait(2);
        sink.on_definition(|cd| {
            let r1 = &cd.data.len();
            assert_eq!(r1, &3); // 3 windows received
            let r2 = &cd.data[0].data;
            assert_eq!(r2, &2); // 1st window for key 1 has 2 elements
            let r3 = &cd.data[1].data;
            assert_eq!(r3, &3); // 2nd window receieved, key 2, has 3 elements
            let r4 = &cd.data[2].data;
            assert_eq!(r4, &1); // 3rd window receieved, for key 3, has 1 elements
        });
    }

    #[test]
    fn window_discard_late_arrival() {
        // Send 2 messages on time, a watermark and a late arrival which is not allowed

        let (assigner_ref, sink) = window_assigner_test_setup(10, 10, 0);
        wait(1);
        // Send messages
        let moment = now();
        assigner_ref.tell(timestamped_event(moment));
        assigner_ref.tell(timestamped_event(moment));
        assigner_ref.tell(watermark(moment + 10));
        wait(1);
        assigner_ref.tell(timestamped_event(moment));
        wait(1);
        // Inspect and assert
        sink.on_definition(|cd| {
            let r1 = &cd.data[0].data;
            assert_eq!(r1, &2);
            let r2 = &cd.data.len();
            assert_eq!(r2, &1);
        });
    }
    #[test]
    fn window_too_late_late_arrival() {
        // Send 2 messages on time, and then 1 message which is too late
        let (assigner_ref, sink) = window_assigner_test_setup(10, 10, 10);
        wait(1);
        // Send messages
        let moment = now();
        assigner_ref.tell(timestamped_event(moment));
        assigner_ref.tell(timestamped_event(moment));
        assigner_ref.tell(watermark(moment + 21));
        wait(1);
        assigner_ref.tell(timestamped_event(moment));
        wait(1);
        // Inspect and assert
        sink.on_definition(|cd| {
            let r0 = &cd.data[0].data;
            assert_eq!(&cd.data.len(), &(1_usize));
            assert_eq!(r0, &2);
        });
    }
    #[test]
    fn window_very_long_windows_1() {
        // Use long windows to check for timer not going out of sync in ms conversion
        let (assigner_ref, sink) = window_assigner_test_setup(10000, 10000, 0);
        wait(1);
        let moment = now();

        // Spawns first window
        assigner_ref.tell(timestamped_event(moment));

        // Spawns second window
        assigner_ref.tell(timestamped_event(moment + 10001));

        // Should only materialize first window
        assigner_ref.tell(watermark(moment + 19999));
        wait(2);
        sink.on_definition(|cd| {
            let r0 = &cd.data[0].data;
            assert_eq!(r0, &1);
            assert_eq!(&cd.data.len(), &(1_usize));
        });
    }
    #[test]
    fn window_very_long_windows_2() {
        // Use long windows to check for timer not going out of alignment
        let (assigner_ref, sink) = window_assigner_test_setup(10000, 10000, 0);
        wait(1);
        let moment = now();

        // Spawns first window
        assigner_ref.tell(timestamped_event(moment));

        // Spawns second window
        assigner_ref.tell(timestamped_event(moment + 10001));

        // Should only materialize first window
        assigner_ref.tell(watermark(moment + 20000));
        wait(2);
        sink.on_definition(|cd| {
            let r0 = &cd.data[0].data;
            assert_eq!(r0, &1);
            assert_eq!(&cd.data.len(), &(2_usize));
            let r1 = &cd.data[1].data;
            assert_eq!(r1, &1);
        });
    }
    #[test]
    fn window_overlapping() {
        // Use overlapping windows (slide = length/2), check that messages appear correctly
        let (assigner_ref, sink) = window_assigner_test_setup(10, 5, 2);
        wait(1);
        // Send messages
        let moment = now();
        assigner_ref.tell(timestamped_event(moment));
        assigner_ref.tell(timestamped_event(moment + 6));
        assigner_ref.tell(timestamped_event(moment + 6));
        assigner_ref.tell(watermark(moment + 23));
        wait(2);
        // Inspect and assert
        sink.on_definition(|cd| {
            //let r2 = &cd.data.len();
            //assert_eq!(r2, &2);
            let r0 = &cd.data[0].data;
            assert_eq!(r0, &3);
            let r1 = &cd.data[1].data;
            assert_eq!(r1, &2);
        });
    }
    #[test]
    fn window_empty() {
        // check that we receive correct number windows from fast forwarding
        let (assigner_ref, sink) = window_assigner_test_setup(5, 5, 0);
        wait(1);
        assigner_ref.tell(watermark(now() + 1));
        assigner_ref.tell(watermark(now() + 7));
        wait(1);
        sink.on_definition(|cd| {
            // The number of windows is hard to assert with dynamic window starts
            //assert_eq!(&sink_inspect.data.len(), &(1 as usize));
            // We should've receieved at least one window which is empty
            let r0 = &cd.data.len();
            assert_eq!(r0, &0);
        });
    }
}
