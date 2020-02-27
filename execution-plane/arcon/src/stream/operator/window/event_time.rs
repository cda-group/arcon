// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    prelude::*,
    stream::operator::{window::WindowContext, OperatorContext},
    util::event_timer::{EventTimer, SerializableEventTimer},
};
use std::{
    collections::hash_map::DefaultHasher,
    hash::{BuildHasher, BuildHasherDefault, Hash, Hasher},
};

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

/// Window Assigner Based on Event Time
///
/// IN: Input event
/// OUT: Output of Window
pub struct EventTimeWindowAssigner<IN, OUT>
where
    IN: ArconType + Hash,
    OUT: ArconType,
{
    // effectively immutable, so no reason to persist
    window_length: u64,
    window_slide: u64,
    late_arrival_time: u64,
    hasher: BuildHasherDefault<DefaultHasher>,
    keyed: bool,

    // window keeps its own state per key and index (via state backend api)
    window: Box<dyn Window<IN, OUT>>,

    // simply persisted state
    // window start has one value per key (via state backend api)
    window_start: BoxedValueState<Timestamp, /*IK=*/ Key, ()>,
    // active windows technically could also use the state backend integration and
    // set IK=Key, N=Index, but I find it clearer this way
    active_windows: BoxedMapState<(Key, Index), ()>,

    // timer is held twice, to avoid serialization overhead - it's a big value
    transient_timer: EventTimer<(Key, Index, Timestamp)>, // Stores key, "index" and timestamp
    persistent_timer: BoxedValueState<SerializableEventTimer<(Key, Index, Timestamp)>>,
}

impl<IN, OUT> EventTimeWindowAssigner<IN, OUT>
where
    IN: 'static + ArconType + Hash,
    OUT: 'static + ArconType,
{
    pub fn new(
        window: Box<dyn Window<IN, OUT>>,
        length: u64,
        slide: u64,
        late: u64,
        keyed: bool,
        state_backend: &mut dyn StateBackend,
    ) -> Self {
        // Sanity check on slide and length
        if length < slide {
            panic!("Window Length lower than slide!");
        }
        if length % slide != 0 {
            panic!("Window Length not divisible by slide!");
        }

        let persistent_timer: BoxedValueState<SerializableEventTimer<(u64, u64, u64)>> =
            state_backend.build("timer").value();

        let transient_timer = match persistent_timer.get(state_backend) {
            Ok(Some(v)) => v.into(),
            Ok(None) => EventTimer::new(),
            Err(e) => panic!("state error: {}", e),
        };

        EventTimeWindowAssigner {
            window_length: length,
            window_slide: slide,
            late_arrival_time: late,
            window,
            hasher: BuildHasherDefault::<DefaultHasher>::default(),
            keyed,

            window_start: state_backend
                .build("window_start")
                .with_init_item_key(0)
                .value(),
            active_windows: state_backend.build("window_active").map(),

            transient_timer,
            persistent_timer,
        }
    }

    // Creates the window trigger for a key and "window index"
    fn new_window_trigger(&mut self, key: Key, index: Index, state_backend: &dyn StateBackend) {
        self.window_start
            .set_current_key(key)
            .expect("window key error");

        let w_start = self
            .window_start
            .get(state_backend)
            .expect("window start state get error")
            .expect("tried to schedule window_trigger for key which hasn't started");

        let ts = w_start + (index * self.window_slide) + self.window_length;

        // Put the window identifier in the timer.
        self.transient_timer
            .schedule_at(ts + self.late_arrival_time, (key, index, ts));
    }

    // Extracts the key from ArconElements
    fn get_key(&mut self, e: &ArconElement<IN>) -> u64 {
        if !self.keyed {
            return 0;
        }
        let mut h = self.hasher.build_hasher();
        e.data.hash(&mut h);
        h.finish()
    }
}

impl<IN, OUT> Operator<IN, OUT> for EventTimeWindowAssigner<IN, OUT>
where
    IN: ArconType + Hash,
    OUT: ArconType,
{
    fn handle_element(&mut self, element: ArconElement<IN>, ctx: OperatorContext<OUT>) {
        let ts = element.timestamp.unwrap_or(1);
        if self.transient_timer.get_time() == 0 {
            // First element received, set the internal timer
            self.transient_timer.set_time(ts);
        }

        if ts
            < self
                .transient_timer
                .get_time()
                .saturating_sub(self.late_arrival_time)
        {
            // Late arrival: early return
            return ();
        }

        let key = self.get_key(&element);
        self.window_start
            .set_current_key(key)
            .expect("window start set key error");

        // Will store the index of the highest and lowest window it should go into
        let mut floor = 0;
        let mut ceil = 0;
        if let Some(start) = self
            .window_start
            .get(ctx.state_backend)
            .expect("window start state get error")
        {
            // Get the highest window the element goes into
            ceil = (ts - start) / self.window_slide;
            if ceil >= (self.window_length / self.window_slide) {
                floor = ceil - (self.window_length / self.window_slide) + 1;
            }
        } else {
            // Window starting now, first element only goes into the first window
            self.window_start
                .set(ctx.state_backend, ts)
                .expect("window start set error");
        }

        // Insert the element into all windows and create new where necessary
        for i in floor..=ceil {
            if let Some(data) = element.data.clone() {
                self.window
                    .on_element(data, WindowContext::new(ctx.state_backend, key, i))
                    .expect("window error");
            }

            if !self
                .active_windows
                .contains(ctx.state_backend, &(key, i))
                .expect("window active check error")
            {
                self.active_windows
                    .insert(ctx.state_backend, (key, i), ())
                    .expect("active windows insert error");
                // Create the window trigger
                self.new_window_trigger(key, i, ctx.state_backend);
            }
        }
    }

    fn handle_watermark(
        &mut self,
        w: Watermark,
        ctx: OperatorContext<OUT>,
    ) -> Option<Vec<ArconEvent<OUT>>> {
        if self
            .active_windows
            .is_empty(ctx.state_backend)
            .expect("state error")
        {
            return None;
        }

        let ts = w.timestamp;
        let windows_to_close = self.transient_timer.advance_to(ts);
        let mut result = Vec::with_capacity(windows_to_close.len());

        for (key, index, timestamp) in windows_to_close {
            let e = self
                .window
                .result(WindowContext::new(ctx.state_backend, key, index))
                .expect("window result error");

            self.window
                .clear(WindowContext::new(ctx.state_backend, key, index))
                .expect("window clear error");
            self.active_windows
                .remove(ctx.state_backend, &(key, index))
                .expect("active window remove error");

            result.push(ArconEvent::Element(ArconElement::with_timestamp(
                e, timestamp,
            )));
        }

        Some(result)
    }

    fn handle_epoch(
        &mut self,
        _epoch: Epoch,
        ctx: OperatorContext<OUT>,
    ) -> Option<ArconResult<Vec<u8>>> {
        self.persistent_timer
            .set(ctx.state_backend, self.transient_timer.inner.clone())
            .expect("state error");
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::channel::{strategy::forward::*, Channel};
    use kompact::prelude::Component;
    use std::{sync::Arc, thread, time, time::UNIX_EPOCH};

    #[arcon_keyed(id)]
    pub struct Item {
        #[prost(uint64, tag = "1")]
        id: u64,
        #[prost(uint32, tag = "2")]
        price: u32,
    }

    // helper functions
    fn window_assigner_test_setup(
        length: u64,
        slide: u64,
        late: u64,
    ) -> (
        ActorRefStrong<ArconMessage<Item>>,
        Arc<Component<DebugNode<u64>>>,
    ) {
        // Kompact set-up
        let system = KompactConfig::default().build().expect("KompactSystem");

        // Create a sink
        let (sink, _) = system.create_and_register(move || DebugNode::new());
        let sink_ref: ActorRefStrong<ArconMessage<u64>> =
            sink.actor_ref().hold().expect("failed to get strong ref");
        let channel_strategy = ChannelStrategy::Forward(Forward::new(
            Channel::Local(sink_ref.clone()),
            NodeID::new(1),
        ));

        fn appender_fn(u: &[Item]) -> u64 {
            u.len() as u64
        }

        let mut state_backend = Box::new(InMemory::new("test").unwrap());

        let window: Box<dyn Window<Item, u64>> =
            Box::new(AppenderWindow::new(&appender_fn, &mut *state_backend));
        let window_assigner = EventTimeWindowAssigner::<Item, u64>::new(
            window,
            length,
            slide,
            late,
            true,
            &mut *state_backend,
        );

        let window_node = system.create(move || {
            Node::<Item, u64>::new(
                1.into(),
                vec![0.into()],
                channel_strategy,
                Box::new(window_assigner),
                state_backend,
            )
        });

        system.start(&window_node);

        let win_ref: ActorRefStrong<ArconMessage<Item>> = window_node
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
    fn watermark(time: u64) -> ArconMessage<Item> {
        ArconMessage::watermark(time, 0.into())
    }
    fn timestamped_event(ts: u64) -> ArconMessage<Item> {
        ArconMessage::element(Item { id: 1, price: 1 }, Some(ts), 0.into())
    }
    fn timestamped_keyed_event(ts: u64, id: u64) -> ArconMessage<Item> {
        ArconMessage::element(Item { id, price: 1 }, Some(ts), 0.into())
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
        let sink_inspect = sink.definition().lock().unwrap();

        let r1 = &sink_inspect.data.len();
        assert_eq!(r1, &3); // 3 windows received
        let r2 = &sink_inspect.data[0].data;
        assert_eq!(r2, &Some(2)); // 1st window for key 1 has 2 elements
        let r3 = &sink_inspect.data[1].data;
        assert_eq!(r3, &Some(3)); // 2nd window receieved, key 2, has 3 elements
        let r4 = &sink_inspect.data[2].data;
        assert_eq!(r4, &Some(1)); // 3rd window receieved, for key 3, has 1 elements
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
        let sink_inspect = sink.definition().lock().unwrap();
        let r1 = &sink_inspect.data[0].data;
        assert_eq!(r1, &Some(2));
        let r2 = &sink_inspect.data.len();
        assert_eq!(r2, &1);
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
        let sink_inspect = sink.definition().lock().unwrap();
        let r0 = &sink_inspect.data[0].data;
        assert_eq!(&sink_inspect.data.len(), &(1 as usize));
        assert_eq!(r0, &Some(2));
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
        let sink_inspect = sink.definition().lock().unwrap();
        let r0 = &sink_inspect.data[0].data;
        assert_eq!(r0, &Some(1));
        assert_eq!(&sink_inspect.data.len(), &(1 as usize));
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
        let sink_inspect = sink.definition().lock().unwrap();
        let r0 = &sink_inspect.data[0].data;
        assert_eq!(r0, &Some(1));
        assert_eq!(&sink_inspect.data.len(), &(2 as usize));
        let r1 = &sink_inspect.data[1].data;
        assert_eq!(r1, &Some(1));
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
        //wait(1);
        // Inspect and assert
        let sink_inspect = sink.definition().lock().unwrap();
        let r2 = &sink_inspect.data.len();
        assert_eq!(r2, &2);
        let r0 = &sink_inspect.data[0].data;
        assert_eq!(r0, &Some(3));
        let r1 = &sink_inspect.data[1].data;
        assert_eq!(r1, &Some(2));
    }
    #[test]
    fn window_empty() {
        // check that we receive correct number windows from fast forwarding
        let (assigner_ref, sink) = window_assigner_test_setup(5, 5, 0);
        wait(1);
        assigner_ref.tell(watermark(now() + 1));
        assigner_ref.tell(watermark(now() + 7));
        wait(1);
        let sink_inspect = sink.definition().lock().unwrap();
        // The number of windows is hard to assert with dynamic window starts
        //assert_eq!(&sink_inspect.data.len(), &(1 as usize));
        // We should've receieved at least one window which is empty
        let r0 = &sink_inspect.data.len();
        assert_eq!(r0, &0);
    }
}
