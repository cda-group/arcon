// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::{
        state::{Backend, Bundle, Handle, MapState, RegistrationToken, Session, ValueState},
        *,
    },
    stream::operator::{window::WindowContext, OperatorContext},
    timer::TimerBackend,
};
use prost::Message;
use std::marker::PhantomData;

/*
    EventTimeWindowAssigner
        * Assigns messages to windows based on event timestamp
        * Time stored as unix timestamps in u64 format (seconds)
        * Windows created on the fly when events for it come in
        * Events need to implement Hash, use "arcon_keyed" macro when setting up the pipeline
*/

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

#[derive(prost::Message, Clone)]
pub struct KeyAndIndex {
    #[prost(uint64)]
    key: Key,
    #[prost(uint64)]
    index: Index,
}

arcon_state::bundle! {
    struct EventTimeWindowAssignerState {
        // window start has one value per key (via state backend api)
        window_start: Handle<ValueState<Timestamp>, Key>,
        // active windows technically could also use the state backend integration and
        // set IK=Key, N=Index, but I find it clearer this way
        active_windows: Handle<MapState<KeyAndIndex, ()>>
    }
}

impl EventTimeWindowAssignerState {
    fn new() -> Self {
        EventTimeWindowAssignerState {
            window_start: Handle::value("window_start").with_item_key(0),
            active_windows: Handle::map("active_windows"),
        }
    }
}

/// Window Assigner Based on Event Time
///
/// IN: Input event
/// OUT: Output of Window
pub struct EventTimeWindowAssigner<IN, OUT, W>
where
    IN: ArconType,
    OUT: ArconType,
    W: Window<IN, OUT>,
{
    // effectively immutable, so no reason to persist
    window_length: u64,
    window_slide: u64,
    late_arrival_time: u64,
    keyed: bool,

    // window keeps its own state per key and index (via state backend api)
    window: W,

    // simply persisted state
    state: EventTimeWindowAssignerState,

    _marker: PhantomData<(IN, OUT)>,
}

impl<IN, OUT, W> EventTimeWindowAssigner<IN, OUT, W>
where
    IN: 'static + ArconType,
    OUT: 'static + ArconType,
    W: Window<IN, OUT>,
{
    pub fn new(window: W, length: u64, slide: u64, late: u64, keyed: bool) -> Self {
        // Sanity check on slide and length
        if length < slide {
            panic!("Window Length lower than slide!");
        }
        if length % slide != 0 {
            panic!("Window Length not divisible by slide!");
        }

        EventTimeWindowAssigner {
            window_length: length,
            window_slide: slide,
            late_arrival_time: late,
            window,
            keyed,

            state: EventTimeWindowAssignerState::new(),
            _marker: Default::default(),
        }
    }

    // Creates the window trigger for a key and "window index"
    fn new_window_trigger(
        &self,
        key: Key,
        index: Index,
        ctx: &mut OperatorContext<Self, impl state::Backend, impl TimerBackend<WindowEvent>>,
    ) -> Result<(), WindowEvent> {
        let mut active_state = self.state.activate(ctx.state_session);
        active_state.window_start().set_item_key(key);

        let w_start = active_state
            .window_start()
            .get()
            .expect("window start state get error")
            .expect("tried to schedule window_trigger for key which hasn't started");

        let ts = w_start + (index * self.window_slide) + self.window_length;

        ctx.schedule_at(
            ts + self.late_arrival_time,
            WindowEvent::new(key, index, ts),
        )
    }

    // Extracts the key from ArconElements
    fn get_key(&self, e: &ArconElement<IN>) -> u64 {
        if !self.keyed {
            return 0;
        }
        e.data.get_key()
    }
}

impl<IN, OUT, W, B> Operator<B> for EventTimeWindowAssigner<IN, OUT, W>
where
    IN: ArconType,
    OUT: ArconType,
    W: Window<IN, OUT>,
    B: Backend,
{
    type IN = IN;
    type OUT = OUT;
    type TimerState = WindowEvent;

    fn register_states(&mut self, registration_token: &mut RegistrationToken<B>) {
        self.state.register_states(registration_token);
        self.window.register_states(registration_token);
    }

    fn init(&mut self, _session: &mut Session<B>) {
        ()
    }
    fn handle_element<CD>(
        &self,
        element: ArconElement<IN>,
        _source: &CD,
        mut ctx: OperatorContext<Self, B, impl TimerBackend<Self::TimerState>>,
    ) where
        CD: ComponentDefinition + Sized + 'static,
    {
        let ts = element.timestamp.unwrap_or(1);

        let time = ctx.current_time();

        let ts_lower_bound = time.saturating_sub(self.late_arrival_time);

        if ts < ts_lower_bound {
            // Late arrival: early return
            return;
        }

        let mut state = self.state.activate(ctx.state_session);
        let key = self.get_key(&element);
        state.window_start().set_item_key(key);

        // Will store the index of the highest and lowest window it should go into
        let mut floor = 0;
        let mut ceil = 0;
        if let Some(start) = state
            .window_start()
            .get()
            .expect("window start state get error")
        {
            // Get the highest window the element goes into
            ceil = (ts - start) / self.window_slide;
            if ceil >= (self.window_length / self.window_slide) {
                floor = ceil - (self.window_length / self.window_slide) + 1;
            }
        } else {
            // Window starting now, first element only goes into the first window
            state
                .window_start()
                .set(ts)
                .expect("window start set error");
        }

        // temporarily deactivate state, so we can borrow the session mutably again
        drop(state);

        // Insert the element into all windows and create new where necessary
        for index in floor..=ceil {
            self.window
                .on_element(
                    element.data.clone(),
                    WindowContext::new(ctx.state_session, key, index),
                )
                .expect("window error");

            let mut state = self.state.activate(ctx.state_session);

            if !state
                .active_windows()
                .contains(&KeyAndIndex { key, index })
                .expect("window active check error")
            {
                state
                    .active_windows()
                    .insert(KeyAndIndex { key, index }, ())
                    .expect("active windows insert error");
                // Create the window trigger
                if let Err(event) = self.new_window_trigger(key, index, &mut ctx) {
                    // I'm pretty sure this shouldn't happen
                    unreachable!("Window was expired when scheduled: {:?}", event);
                }
            }
        }
    }

    crate::ignore_watermark!(B);
    crate::ignore_epoch!(B);

    fn handle_timeout<CD>(
        &self,
        timeout: Self::TimerState,
        source: &CD,
        mut ctx: OperatorContext<Self, B, impl TimerBackend<Self::TimerState>>,
    ) where
        CD: ComponentDefinition + Sized + 'static,
    {
        let WindowEvent {
            key,
            index,
            timestamp,
        } = timeout;

        let e = self
            .window
            .result(WindowContext::new(ctx.state_session, key, index))
            .expect("window result error");

        self.window
            .clear(WindowContext::new(ctx.state_session, key, index))
            .expect("window clear error");
        self.state
            .activate(ctx.state_session)
            .active_windows()
            .remove(&KeyAndIndex { key, index })
            .expect("active window remove error");

        let window_result = ArconEvent::Element(ArconElement::with_timestamp(e, timestamp));
        ctx.output(window_result, source);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        state::InMemory,
        stream::channel::{strategy::forward::*, Channel},
        timer,
    };
    use kompact::prelude::Component;
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
        let mut pipeline = ArconPipeline::new();
        let pool_info = pipeline.get_pool_info();
        let system = pipeline.system();

        // Create a sink
        let (sink, _) = system.create_and_register(move || DebugNode::new());
        let sink_ref: ActorRefStrong<ArconMessage<u64>> =
            sink.actor_ref().hold().expect("failed to get strong ref");

        let channel_strategy = ChannelStrategy::Forward(Forward::new(
            Channel::Local(sink_ref.clone()),
            NodeID::new(1),
            pool_info,
        ));

        fn appender_fn(u: &[u64]) -> u64 {
            u.len() as u64
        }

        let state_backend = InMemory::create("test".as_ref()).unwrap();

        let window = AppenderWindow::new(&appender_fn);
        let window_assigner = EventTimeWindowAssigner::new(window, length, slide, late, true);

        let window_node = system.create(move || {
            Node::new(
                String::from("window_node"),
                1.into(),
                vec![0.into()],
                channel_strategy,
                window_assigner,
                state_backend,
                timer::wheel(),
            )
        });

        system.start(&window_node);

        let win_ref: ActorRefStrong<ArconMessage<u64>> = window_node
            .actor_ref()
            .hold()
            .expect("failed to get strong ref");
        system.start(&sink);
        system.start(&window_node);
        return (win_ref, sink);
    }
    fn now() -> u64 {
        return time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("error")
            .as_secs();
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
        wait(1);
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
            assert_eq!(&cd.data.len(), &(1 as usize));
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
        wait(1);
        sink.on_definition(|cd| {
            let r0 = &cd.data[0].data;
            assert_eq!(r0, &1);
            assert_eq!(&cd.data.len(), &(1 as usize));
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
        wait(1);
        sink.on_definition(|cd| {
            let r0 = &cd.data[0].data;
            assert_eq!(r0, &1);
            assert_eq!(&cd.data.len(), &(2 as usize));
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
        wait(1);
        // Inspect and assert
        sink.on_definition(|cd| {
            let r2 = &cd.data.len();
            assert_eq!(r2, &2);
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
