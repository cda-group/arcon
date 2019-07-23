use crate::data::{ArconElement, ArconType};
use crate::messages::protobuf::*;
use crate::streaming::partitioner::Partitioner;
use crate::streaming::window::builder::{WindowBuilder, WindowFn, WindowModules};
use crate::util::event_timer::{EventTimer, ExecuteAction};
use crate::weld::module::Module;
use kompact::*;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};
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
/// P: Port type for the `Partitioner`
#[derive(ComponentDefinition)]
pub struct EventTimeWindowAssigner<IN, FUNC, OUT, P>
where
    IN: 'static + ArconType + Hash,
    FUNC: 'static + Clone,
    OUT: 'static + ArconType,
    P: Port<Request = ArconElement<OUT>> + 'static + Clone,
{
    ctx: ComponentContext<Self>,
    partitioner: Box<Partitioner<OUT, P, Self>>,
    window_length: u64,
    window_slide: u64,
    late_arrival_time: u64,
    window_start: HashMap<u64, u64>,
    window_maps: HashMap<u64, HashMap<u64, WindowBuilder<IN, FUNC, OUT>>>,
    window_modules: WindowModules,
    timer: Box<EventTimer<Self>>,
    hasher: BuildHasherDefault<DefaultHasher>,
}

impl<IN, FUNC, OUT, P> EventTimeWindowAssigner<IN, FUNC, OUT, P>
where
    IN: 'static + ArconType + Hash,
    FUNC: 'static + Clone,
    OUT: 'static + ArconType,
    P: Port<Request = ArconElement<OUT>> + 'static + Clone,
{
    pub fn new(
        partitioner: Box<Partitioner<OUT, P, Self>>,
        init_builder_code: String,
        udf_code: String,
        result_code: String,
        length: u64,
        slide: u64,
        late: u64,
    ) -> Self {
        let init_builder = Arc::new(Module::new(init_builder_code).unwrap());
        let code_module = Arc::new(Module::new(udf_code).unwrap());
        let result_module = Arc::new(Module::new(result_code).unwrap());

        let window_modules = WindowModules {
            init_builder,
            udf: code_module,
            materializer: result_module,
        };

        // TODO: Should handle weld compilation errors here

        // Sanity check on slide and length
        if length < slide {
            panic!("Window Length lower than slide!");
        }
        if length % slide != 0 {
            panic!("Window Length not divisible by slide!");
        }

        EventTimeWindowAssigner {
            ctx: ComponentContext::new(),
            partitioner,
            window_length: length,
            window_slide: slide,
            late_arrival_time: late,
            window_start: HashMap::new(),
            window_maps: HashMap::new(),
            window_modules,
            timer: Box::new(EventTimer::new()),
            hasher: BuildHasherDefault::<DefaultHasher>::default(),
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
        debug!(
            self.ctx.log(),
            "creating new window for key {}, timestamp {}", key, ts
        );

        // Schedule trigger
        self.timer
            .schedule_at(ts + self.late_arrival_time, move |self_c, _| {
                let self_ptr = self_c as *const Self;

                if let Some(w_map) = self_c.window_maps.get_mut(&key) {
                    match w_map.remove(&index) {
                        Some(mut window) => {
                            match window.result() {
                                Ok(e) => {
                                    debug!(self_c.ctx.log(), "Window {} result materialized!", ts);
                                    if let Err(_err) = self_c
                                        .partitioner
                                        .output(ArconElement::new(e), self_ptr, Some(ts)) {
                                            error!(
                                                self_c.ctx.log(),
                                                "Failed to send window result"
                                            );
                                        }
                                }
                                _ => {
                                    error!(
                                        self_c.ctx.log(),
                                        "failed to get result, couldn't materialize from window_builder"
                                    );
                                }
                            }
                        }
                        None => {
                            error!(self_c.ctx.log(), "No window found for key {} and timestamp {}", key, ts);
                        }
                    }
                } else {
                    error!(
                        self_c.ctx.log(),
                        "failed to get result, couldn't find the w_map for key {}", key
                    );
                }
            });
    }
    // Extracts the key from ArconElements
    fn get_key(&mut self, e: ArconElement<IN>) -> u64 {
        let mut h = self.hasher.build_hasher();
        e.data.hash(&mut h);
        return h.finish();
    }
    fn handle_element(&mut self, e: ArconElement<IN>) -> () {
        let ts = e.timestamp.unwrap_or(0);
        if self.window_start.is_empty() {
            // First element received, set the internal timer
            self.timer.set_time(ts);
        }
        if ts < self.timer.get_time() - self.late_arrival_time {
            // Discard late arrival
            return;
        }

        let key = self.get_key(e);
        debug!(
            self.ctx.log(),
            "handling element with timestamp: {}, key: {}", ts, key
        );

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
                    if let Err(_err) = window.on_element(e.data) {
                        error!(self.ctx.log(), "Error inserting element");
                    }
                }
                None => {
                    // Need to create new window,
                    let mut window: WindowBuilder<IN, FUNC, OUT> =
                        WindowBuilder::new(self.window_modules.clone()).unwrap();
                    if let Err(_err) = window.on_element(e.data) {
                        error!(self.ctx.log(), "Error inserting element");
                    }
                    w_map.insert(i, window);
                    // Create the window trigger
                    self.new_window_trigger(key, i);
                }
            }
        }
        self.window_maps.insert(key, w_map);
    }
    fn handle_watermark(&mut self, w: &Watermark) -> () {
        //debug!(self.ctx.log(), "handling watermark with timestamp: {}", w);
        let ts = w.get_timestamp();

        // timer returns a set of executable actions
        let actions = self.timer.advance_to(ts);
        for a in actions {
            match a {
                ExecuteAction::Once(id, action) => {
                    action(self, id);
                }
                ExecuteAction::Periodic(id, action) => {
                    action(self, id);
                }
                ExecuteAction::None => {}
            }
        }
        // TODO: fwd watermark
        // self.partitioner.output(w, self as *const Self, Some(ts));
    }
}

impl<IN, FUNC, OUT, P> Require<P> for EventTimeWindowAssigner<IN, FUNC, OUT, P>
where
    IN: 'static + ArconType + Hash,
    FUNC: 'static + Clone,
    OUT: 'static + ArconType,
    P: Port<Request = ArconElement<OUT>> + 'static + Clone,
{
    fn handle(&mut self, _event: P::Indication) -> () {
        // ignore
    }
}

impl<IN, FUNC, OUT, P> Provide<ControlPort> for EventTimeWindowAssigner<IN, FUNC, OUT, P>
where
    IN: 'static + ArconType + Hash,
    FUNC: 'static + Clone,
    OUT: 'static + ArconType,
    P: Port<Request = ArconElement<OUT>> + 'static + Clone,
{
    fn handle(&mut self, event: ControlEvent) -> () {
        if let ControlEvent::Start = event {}
    }
}

impl<IN, FUNC, OUT, P> Actor for EventTimeWindowAssigner<IN, FUNC, OUT, P>
where
    IN: 'static + ArconType + Hash,
    FUNC: 'static + Clone,
    OUT: 'static + ArconType,
    P: Port<Request = ArconElement<OUT>> + 'static + Clone,
{
    fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
        if let Some(e) = msg.downcast_ref::<ArconElement<IN>>() {
            self.handle_element(*e);
        } else if let Some(w) = msg.downcast_ref::<Watermark>() {
            self.handle_watermark(w);
        } else {
            error!(self.ctx.log(), "Unrecognized message from {:?}", _sender);
        }
    }
    fn receive_message(&mut self, _sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {
        // This remote message receiver is untested and probably doesn't work
        /*
        if ser_id == serialisation_ids::PBUF {
            let r: Result<StreamTaskMessage, SerError> = ProtoSer::deserialise(buf);
            if let Ok(msg) = r {
                let payload = msg.payload.unwrap();
                match payload {
                    element(e) => {
                        // todo: convert from protobuff to ArconElement
                        //self.handle_element(e);
                    }
                    keyed_element(_) => {}
                    watermark(w) => {
                        self.handle_watermark(&w);
                    }
                    checkpoint(_) => {
                        // TODO: Persistant State
                    }
                }
            } else {
                error!(self.ctx.log(), "Failed to deserialize StreamTaskMessage",);
            }
        } else {
            debug!(self.ctx.log(), "Unrecognized remote message",);
        }
        */
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::partitioner::forward::*;
    use crate::streaming::partitioner::*;
    use crate::streaming::{Channel, ChannelPort};
    use kompact::default_components::DeadletterBox;
    use std::cell::UnsafeCell;
    use std::rc::Rc;
    use std::time::UNIX_EPOCH;
    use std::{thread, time};
    use weld::data::Appender;
    use weld::data::WeldVec;

    // Stub for window-results
    mod sink {
        use super::*;

        #[derive(ComponentDefinition)]
        pub struct Sink {
            ctx: ComponentContext<Sink>,
            pub sink_port: ProvidedPort<ChannelPort<WindowOutput>, Self>,
            pub result: Vec<Option<i64>>,
        }
        impl Sink {
            pub fn new() -> Sink {
                Sink {
                    ctx: ComponentContext::new(),
                    sink_port: ProvidedPort::new(),
                    result: Vec::new(),
                }
            }
        }
        impl Provide<ControlPort> for Sink {
            fn handle(&mut self, _event: ControlEvent) -> () {}
        }
        impl Actor for Sink {
            fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
                if let Some(m) = msg.downcast_ref::<ArconElement<WindowOutput>>() {
                    println!("Sink got msg {}", m.data.len);
                    self.result.push(Some(m.data.len));
                }
            }
            fn receive_message(&mut self, _sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {}
        }
        impl Provide<ChannelPort<WindowOutput>> for Sink {
            fn handle(&mut self, event: ArconElement<WindowOutput>) -> () {
                self.result.push(Some(event.data.len));
            }
        }
        impl Require<ChannelPort<WindowOutput>> for Sink {
            fn handle(&mut self, event: ()) -> () {
                // ignore
            }
        }
    }

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
    ) -> (ActorRef, Arc<kompact::Component<sink::Sink>>) {
        // Kompact set-up
        let cfg = KompactConfig::new();
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        // Create a sink
        let (sink, _) = system.create_and_register(move || sink::Sink::new());
        let sink_ref = sink.actor_ref();

        pub type Assigner =
            EventTimeWindowAssigner<Item, Appender<u32>, WindowOutput, ChannelPort<WindowOutput>>;

        let mut partitioner: Box<Forward<WindowOutput, ChannelPort<WindowOutput>, Assigner>> =
            Box::new(Forward::new(Channel::Local(sink_ref.clone())));

        // Create the window_assigner
        let builder_code = String::from("|| appender[u32]");
        let udf_code = String::from("|x: {u64, u32}, y: appender[u32]| merge(y, x.$1)");
        let udf_result = String::from("|y: appender[u32]| len(result(y))");
        let window_assigner = EventTimeWindowAssigner::new(
            partitioner,
            builder_code,
            udf_code,
            udf_result,
            length,
            slide,
            late,
        );

        let (assigner, _) = system.create_and_register(move || window_assigner);
        let win_ref = assigner.actor_ref();
        system.start(&sink);
        system.start(&assigner);
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
    fn watermark(time: u64) -> Watermark {
        let mut w = Watermark::new();
        w.set_timestamp(time);
        return w;
    }
    fn timestamped_event(ts: u64) -> Box<ArconElement<Item>> {
        return Box::new(ArconElement {
            data: Item { id: 1, price: 1 },
            timestamp: Some(ts),
        });
    }
    fn timestamped_keyed_event(ts: u64, id: u64) -> Box<ArconElement<Item>> {
        return Box::new(ArconElement {
            data: Item { id, price: 1 },
            timestamp: Some(ts),
        });
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
        let mut sink_inspect = sink.definition().lock().unwrap();

        let r1 = &sink_inspect.result.len();
        assert_eq!(r1, &3); // 3 windows received
        let r2 = &sink_inspect.result[0].take().unwrap();
        assert_eq!(r2, &2); // 1st window for key 1 has 2 elements
        let r3 = &sink_inspect.result[1].take().unwrap();
        assert_eq!(r3, &3); // 2nd window receieved, key 2, has 3 elements
        let r4 = &sink_inspect.result[2].take().unwrap();
        assert_eq!(r2, &2); // 3rd window receieved, for key 3, has 1 elements
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
        let mut sink_inspect = sink.definition().lock().unwrap();
        let r1 = &sink_inspect.result[0].take().unwrap();
        assert_eq!(r1, &2);
        let r2 = &sink_inspect.result.len();
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
        let mut sink_inspect = sink.definition().lock().unwrap();
        let r0 = &sink_inspect.result[0].take().unwrap();
        assert_eq!(&sink_inspect.result.len(), &(1 as usize));
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
        let mut sink_inspect = sink.definition().lock().unwrap();
        let r0 = &sink_inspect.result[0].take().unwrap();
        assert_eq!(r0, &1);
        assert_eq!(&sink_inspect.result.len(), &(1 as usize));
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
        let mut sink_inspect = sink.definition().lock().unwrap();
        let r0 = &sink_inspect.result[0].take().unwrap();
        assert_eq!(r0, &1);
        assert_eq!(&sink_inspect.result.len(), &(2 as usize));
        let r1 = &sink_inspect.result[1].take().unwrap();
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
        let mut sink_inspect = sink.definition().lock().unwrap();
        let r2 = &sink_inspect.result.len();
        assert_eq!(r2, &2);
        let r0 = &sink_inspect.result[0].take().unwrap();
        assert_eq!(r0, &3);
        let r1 = &sink_inspect.result[1].take().unwrap();
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
        let mut sink_inspect = sink.definition().lock().unwrap();
        // The number of windows is hard to assert with dynamic window starts
        //assert_eq!(&sink_inspect.result.len(), &(1 as usize));
        // We should've receieved at least one window which is empty
        let r0 = &sink_inspect.result.len();
        assert_eq!(r0, &0);
    }

    /// Similar to the above tests. Only major
    /// difference is that the following test uses
    /// uses a Port channel to send the window result to.
    #[test]
    fn window_port_channel_test() {
        let length = 5;
        let slide = 5;

        let mut cfg = KompactConfig::new();
        cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        // Create a sink and create port channel
        let (sink, _) = system.create_and_register(move || sink::Sink::new());
        let target_port = sink.on_definition(|c| c.sink_port.share());
        use crate::streaming::window::assigner::tests::sink::Sink;

        pub type Assigner =
            EventTimeWindowAssigner<Item, Appender<u32>, WindowOutput, ChannelPort<WindowOutput>>;
        let mut req_port: RequiredPort<ChannelPort<WindowOutput>, Assigner> = RequiredPort::new();
        let _ = req_port.connect(target_port);
        let ref_port = crate::streaming::RequirePortRef(Rc::new(UnsafeCell::new(req_port)));
        let comp_channel: Channel<WindowOutput, ChannelPort<WindowOutput>, Assigner> =
            Channel::Port(ref_port);

        let partitioner: Box<Forward<WindowOutput, ChannelPort<WindowOutput>, Assigner>> =
            Box::new(Forward::new(Channel::Local(sink.actor_ref().clone())));

        // Create the window_assigner

        let builder_code = String::from("|| appender[u32]");
        let udf_code = String::from("|x: {u64, u32}, y: appender[u32]| merge(y, x.$1)");
        let udf_result = String::from("|y: appender[u32]| len(result(y))");
        let window_assigner = EventTimeWindowAssigner::new(
            partitioner,
            builder_code,
            udf_code,
            udf_result,
            length,
            slide,
            0,
        );

        // Register and start components
        let (assigner, _) = system.create_and_register(move || window_assigner);
        let assigner_ref = assigner.actor_ref();
        system.start(&sink);
        system.start(&assigner);

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
        let mut sink_inspect = sink.definition().lock().unwrap();
        let result = &sink_inspect.result[0].take().unwrap();
        assert_eq!(result, &2);
    }
}
