use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};
use std::collections::HashMap;
use crate::util::event_timer::EventTimer;
use std::collections::hash_map::DefaultHasher;
use crate::prelude::*;
use std::sync::Arc;

/*
    EventTimeWindowAssigner
        * Assigns messages to windows based on event timestamp
        * Time stored as unix timestamps in u64 format (seconds)
        * Windows created on the fly when events for it come in
        * Events need to implement Hash, use "KeyBy" macro when setting up the pipeline
*/

/// Window Assigner that manages and triggers `WindowComponent`s
///
/// IN: Input event
/// FUNC: ´WindowBuilder`s internal builder type
/// OUT: Output of Window
pub struct EventTimeWindowAssigner<IN, FUNC, OUT>
where
    IN: 'static + ArconType + Hash,
    FUNC: 'static + Clone,
    OUT: 'static + ArconType,
{
    window_length: u64,
    window_slide: u64,
    late_arrival_time: u64,
    window_start: HashMap<u64, u64>,
    window_maps: HashMap<u64, HashMap<u64, WindowBuilder<IN, FUNC, OUT>>>,
    window_modules: WindowModules,
    timer: Box<EventTimer<(u64, u64, u64)>>,  // Stores key, "index" and timestamp
    hasher: BuildHasherDefault<DefaultHasher>,
    keyed: bool,
}

impl<IN, FUNC, OUT> EventTimeWindowAssigner<IN, FUNC, OUT>
where
    IN: 'static + ArconType + Hash,
    FUNC: 'static + Clone,
    OUT: 'static + ArconType,
{
    pub fn new(
        init_builder_code: String,
        udf_code: String,
        result_code: String,
        length: u64,
        slide: u64,
        late: u64,
        keyed: bool,
    ) -> Self {
        let init_builder = Arc::new(Module::new(init_builder_code).unwrap());
        let code_module = Arc::new(Module::new(udf_code).unwrap());
        let result_module = Arc::new(Module::new(result_code).unwrap());

        let window_modules = WindowModules {
            init_builder,
            udf: code_module,
            materializer: result_module,
        };

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
            window_start: HashMap::new(),
            window_maps: HashMap::new(),
            window_modules,
            timer: Box::new(EventTimer::new()),
            hasher: BuildHasherDefault::<DefaultHasher>::default(),
            keyed,
        }
    }

    // Creates the window trigger for a key and "window index"
    fn new_window_trigger(&mut self, key: u64, index: u64) -> () {
        let w_start = match self.window_start.get(&key) {
            Some(start) => start,
            None => {
                panic!("tried to schedule window_trigger for key which hasn't started");
            }
        };
        let ts = w_start + (index * self.window_slide) + self.window_length;

        // Put the window identifier in the timer.
        self.timer.schedule_at(ts + self.late_arrival_time, (key, index, ts));
    }
    // Extracts the key from ArconElements
    fn get_key(&mut self, e: ArconElement<IN>) -> u64 {
        if !self.keyed {
            return 0;
        }
        let mut h = self.hasher.build_hasher();
        e.data.hash(&mut h);
        return h.finish();
    }

}

impl<IN, OUT, FUNC> Task<IN, OUT> for EventTimeWindowAssigner<IN, FUNC, OUT>
where
    IN: 'static + ArconType + Hash,
    FUNC: 'static + Clone,
    OUT: 'static + ArconType,
{
    fn handle_element(&mut self, element: ArconElement<IN>) -> ArconResult<Vec<ArconEvent<OUT>>> {
        let ts = element.timestamp.unwrap_or(0);
        if self.window_start.is_empty() {
            // First element received, set the internal timer
            self.timer.set_time(ts);
        }
        if ts < self.timer.get_time() - self.late_arrival_time {
            // Late arrival: early return
            return Ok(Vec::new());
        }

        let key = self.get_key(element);

        // Will store the index of the highest and lowest window it should go into
        let mut floor = 0;
        let mut ceil = 0;
        if let Some(start) = self.window_start.get(&key) {
            // Get the highest window the element goes into
            ceil = (ts - start) / self.window_slide;
            if ceil >= (self.window_length / self.window_slide) {
                floor = ceil - (self.window_length / self.window_slide) + 1;
            }
        } else {
            // Window starting now, first element only goes into the first window
            self.window_start.insert(key, ts);
        }

        // Take the w_map or make a new one and insert when we're done
        let mut w_map = match self.window_maps.remove(&key) {
            Some(map) => map,
            None => HashMap::new(),
        };

        // Insert the element into all windows and create new where necassery
        for i in floor..=ceil {
            match w_map.get_mut(&i) {
                Some(window) => {
                    // Just insert the element
                    if let Err(err) = window.on_element(element.data) {
                        return Err(err);
                    }
                }
                None => {
                    // Need to create new window,
                    let mut window: WindowBuilder<IN, FUNC, OUT> =
                        WindowBuilder::new(self.window_modules.clone()).unwrap();
                    if let Err(err) = window.on_element(element.data) {
                        return Err(err);
                    }
                    w_map.insert(i, window);
                    // Create the window trigger
                    self.new_window_trigger(key, i);
                }
            }
        }
        self.window_maps.insert(key, w_map);
        Ok(Vec::new())
    }
    fn handle_watermark(&mut self, w: Watermark) -> ArconResult<Vec<ArconEvent<OUT>>> {
        if self.window_start.is_empty() {
            // Early return
            return Ok(Vec::new());
        }
        let ts = w.timestamp;        

        // timer returns a set of (key, index, timestamp) identifying what windows to close
        let windows = self.timer.advance_to(ts);
        let mut result = Vec::new();
        for (key, index, timestamp) in windows {
            if let Some(w_map) = self.window_maps.get_mut(&key) {
                match w_map.remove(&index) {
                    Some(mut window) => {
                        match window.result() {
                            Ok(e) => {
                                result.push(ArconEvent::Element(
                                    ArconElement::with_timestamp(e, timestamp)
                                ))
                            }
                            _ => {}
                        }
                    }
                    None => {}
                }
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::channel::strategy::forward::*;
    use crate::streaming::channel::Channel;
    use std::time::UNIX_EPOCH;
    use std::{thread, time};
    use weld::data::Appender;

    #[arcon]
    #[derive(Hash)]
    pub struct WindowOutput {
        pub len: i64,
    }

    // helper functions
    fn window_assigner_test_setup(
        length: u64,
        slide: u64,
        late: u64,
    ) -> (ActorRef, Arc<kompact::Component<DebugSink<WindowOutput>>>) {
        // Kompact set-up
        let cfg = KompactConfig::new();
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        // Create a sink
        let (sink, _) = system.create_and_register(move || DebugSink::new());
        let sink_ref = sink.actor_ref();

        let channel_strategy: Box<Forward<WindowOutput>> =
            Box::new(Forward::new(Channel::Local(sink_ref.clone())));

        // Create the window_assigner
        let builder_code = String::from("|| appender[u32]");
        let udf_code = String::from("|x: {u64, u32}, y: appender[u32]| merge(y, x.$1)");
        let udf_result = String::from("|y: appender[u32]| len(result(y))");
        

        let window_assigner = EventTimeWindowAssigner::<Item, Appender<u32>, WindowOutput>::new(
            builder_code,
            udf_code,
            udf_result,
            length,
            slide,
            late,
            true,
        );

        let window_node = system.create_and_start(move || {
            Node::<Item, WindowOutput>::new(
                "node1".to_string(),
                vec!("test".to_string()),
                channel_strategy,
                Box::new(window_assigner)
            )
        });

        let win_ref = window_node.actor_ref();
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
    fn wait(time: u64) -> () {
        thread::sleep(time::Duration::from_secs(time));
    }
    fn watermark(time: u64) -> ArconMessage<Item> {
        ArconMessage::watermark(time, "test".to_string())
    }
    fn timestamped_event(ts: u64) -> Box<ArconMessage<Item>> {
        Box::new(ArconMessage::element(Item{id:1, price: 1}, Some(ts), "test".to_string()))
    }
    fn timestamped_keyed_event(ts: u64, id: u64) -> Box<ArconMessage<Item>> {
        Box::new(ArconMessage::element(Item{id, price: 1}, Some(ts), "test".to_string()))
    }
    #[key_by(id)]
    #[arcon]
    pub struct Item {
        id: u64,
        price: u32,
    }

    // Tests:
    #[test]
    fn window_by_key() {
        let (assigner_ref, sink) = window_assigner_test_setup(10, 5, 0);
        wait(1);
        let moment = now();
        assigner_ref.tell(timestamped_keyed_event(moment, 1), &assigner_ref);
        assigner_ref.tell(timestamped_keyed_event(moment + 1, 2), &assigner_ref);
        assigner_ref.tell(timestamped_keyed_event(moment + 2, 3), &assigner_ref);
        assigner_ref.tell(timestamped_keyed_event(moment + 3, 2), &assigner_ref);
        assigner_ref.tell(timestamped_keyed_event(moment + 5, 2), &assigner_ref);
        assigner_ref.tell(timestamped_keyed_event(moment + 4, 1), &assigner_ref);

        wait(1);
        assigner_ref.tell(Box::new(watermark(moment + 12)), &assigner_ref);
        wait(1);
        let sink_inspect = sink.definition().lock().unwrap();

        let r1 = &sink_inspect.data.len();
        assert_eq!(r1, &3); // 3 windows received
        let r2 = &sink_inspect.data[0].data.len;
        assert_eq!(r2, &2); // 1st window for key 1 has 2 elements
        let r3 = &sink_inspect.data[1].data.len;
        assert_eq!(r3, &3); // 2nd window receieved, key 2, has 3 elements
        let r4 = &sink_inspect.data[2].data.len;
        assert_eq!(r4, &1); // 3rd window receieved, for key 3, has 1 elements
    }

    #[test]
    fn window_discard_late_arrival() {
        // Send 2 messages on time, a watermark and a late arrival which is not allowed

        let (assigner_ref, sink) = window_assigner_test_setup(10, 10, 0);
        wait(1);
        // Send messages
        let moment = now();
        assigner_ref.tell(timestamped_event(moment), &assigner_ref);
        assigner_ref.tell(timestamped_event(moment), &assigner_ref);
        assigner_ref.tell(Box::new(watermark(moment + 10)), &assigner_ref);
        wait(1);
        assigner_ref.tell(timestamped_event(moment), &assigner_ref);
        wait(1);
        // Inspect and assert
        let sink_inspect = sink.definition().lock().unwrap();
        let r1 = &sink_inspect.data[0].data.len;
        assert_eq!(r1, &2);
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
        assigner_ref.tell(timestamped_event(moment), &assigner_ref);
        assigner_ref.tell(timestamped_event(moment), &assigner_ref);
        assigner_ref.tell(Box::new(watermark(moment + 21)), &assigner_ref);
        wait(1);
        assigner_ref.tell(timestamped_event(moment), &assigner_ref);
        wait(1);
        // Inspect and assert
        let sink_inspect = sink.definition().lock().unwrap();
        let r0 = &sink_inspect.data[0].data.len;
        assert_eq!(&sink_inspect.data.len(), &(1 as usize));
        assert_eq!(r0, &2);
    }
    #[test]
    fn window_very_long_windows_1() {
        // Use long windows to check for timer not going out of sync in ms conversion
        let (assigner_ref, sink) = window_assigner_test_setup(10000, 10000, 0);
        wait(1);
        let moment = now();

        // Spawns first window
        assigner_ref.tell(timestamped_event(moment), &assigner_ref);

        // Spawns second window
        assigner_ref.tell(timestamped_event(moment + 10001), &assigner_ref);

        // Should only materialize first window
        assigner_ref.tell(Box::new(watermark(moment + 19999)), &assigner_ref);
        wait(1);
        let sink_inspect = sink.definition().lock().unwrap();
        let r0 = &sink_inspect.data[0].data.len;
        assert_eq!(r0, &1);
        assert_eq!(&sink_inspect.data.len(), &(1 as usize));
    }
    #[test]
    fn window_very_long_windows_2() {
        // Use long windows to check for timer not going out of alignment
        let (assigner_ref, sink) = window_assigner_test_setup(10000, 10000, 0);
        wait(1);
        let moment = now();

        // Spawns first window
        assigner_ref.tell(timestamped_event(moment), &assigner_ref);

        // Spawns second window
        assigner_ref.tell(timestamped_event(moment + 10001), &assigner_ref);

        // Should only materialize first window
        assigner_ref.tell(Box::new(watermark(moment + 20000)), &assigner_ref);
        wait(1);
        let sink_inspect = sink.definition().lock().unwrap();
        let r0 = &sink_inspect.data[0].data.len;
        assert_eq!(r0, &1);
        assert_eq!(&sink_inspect.data.len(), &(2 as usize));
        let r1 = &sink_inspect.data[1].data.len;
        assert_eq!(r1, &1);
    }
    #[test]
    fn window_overlapping() {
        // Use overlapping windows (slide = length/2), check that messages appear correctly
        let (assigner_ref, sink) = window_assigner_test_setup(10, 5, 2);
        wait(1);
        // Send messages
        let moment = now();
        assigner_ref.tell(timestamped_event(moment), &assigner_ref);
        assigner_ref.tell(timestamped_event(moment + 6), &assigner_ref);
        assigner_ref.tell(timestamped_event(moment + 6), &assigner_ref);
        //assigner_ref.tell(Box::new(watermark(moment + 12)), &assigner_ref);
        assigner_ref.tell(Box::new(watermark(moment + 23)), &assigner_ref);
        wait(1);
        //wait(1);
        // Inspect and assert
        let sink_inspect = sink.definition().lock().unwrap();
        let r2 = &sink_inspect.data.len();
        assert_eq!(r2, &2);
        let r0 = &sink_inspect.data[0].data.len;
        assert_eq!(r0, &3);
        let r1 = &sink_inspect.data[1].data.len;
        assert_eq!(r1, &2);
    }
    #[test]
    fn window_empty() {
        // check that we receive correct number windows from fast forwarding
        let (assigner_ref, sink) = window_assigner_test_setup(5, 5, 0);
        wait(1);
        assigner_ref.tell(Box::new(watermark(now() + 1)), &assigner_ref);
        assigner_ref.tell(Box::new(watermark(now() + 7)), &assigner_ref);
        wait(1);
        let sink_inspect = sink.definition().lock().unwrap();
        // The number of windows is hard to assert with dynamic window starts
        //assert_eq!(&sink_inspect.data.len(), &(1 as usize));
        // We should've receieved at least one window which is empty
        let r0 = &sink_inspect.data.len();
        assert_eq!(r0, &0);
    }
}
