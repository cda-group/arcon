use crate::data::{ArconElement, ArconType};
use crate::prelude::{DeserializeOwned, Serialize};
use crate::streaming::partitioner::Partitioner;
use crate::streaming::window::builder::{WindowBuilder, WindowFn, WindowModules};
use crate::util::event_timer::{EventTimer, ExecuteAction};
use crate::weld::module::Module;
use kompact::timer::Timer;
use kompact::*;
use messages::protobuf::StreamTaskMessage_oneof_payload::*;
use messages::protobuf::*;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;
use std::{thread, time};
use uuid::Uuid;
/*
    EventTimeWindowAssigner
        * Assigns messages to windows based on event timestamp
        * Time stored as unix timestamps in u64 format (seconds)
        * Windows stored in sorted btree represented by the end time of the window
        * Supports late arrivals, components will send new result for each late event
        * Requires watermarks to complete windows
        * Windows don't actually "die" currently
    Tests:
        Tests assigner and components together
        Uses a weld appender in all tests and compares lengths
        Tests utilize watermarks to test processesing time
        Tests both tumbling and sliding windows

    TOOD:
        Ensure Window output order
        Add persistent state with checkpoints
        Handle weld compilation errors
        Use generalized/common message format/objects <- Element is not generic
        More tests
    Configuration considerations:
        Early arrivals before watermarks may be discarded, what to do with them?
            Currently only allows window_assigner to "fast-forward" on watermarks
        Change time format to milliseconds?
        Standardise the message formats?
            Can they be disseminated better through the components if we do?
        WindowBy parameter allowing for EventTime/ProcessingTime/IngestionTime?
        Flexible late-arrival policy?
            Send new result per each late or just once before window closes?
            Diff / New result / something else?
        Watermark skipping ahead in line question:
            Kill event, ie. watermarks "jumps" the event queue (due to control-event),
            meaning events currently in the event queue of a window component may be
            deleted.
            Is this wanted behaviour, could be good for times where result delivery
            matters more than completeness?
            See test case overlapping_windows for the "determinism issue" (late arrivals)
*/

/// Window Assigner that manages and triggers `WindowComponent`s
///
/// A: Input event
/// B: ´WindowBuilder`s internal builder type
/// C: Output of Window
/// D: Port type for the `Partitioner`
pub struct EventTimeWindowAssigner<A, B, C, D>
where
    A: 'static + ArconType,
    B: 'static + Clone,
    C: 'static + ArconType,
    D: Port<Request = ArconElement<C>> + 'static + Clone,
{
    ctx: ComponentContext<Self>,
    partitioner: Box<Partitioner<C, D, Self>>,
    window_count: u64,
    window_length: u64,
    window_slide: u64,
    window_start: u64,
    late_arrival_time: u64,
    max_window_ts: u64,
    max_watermark: u64,
    window_map: BTreeMap<u64, WindowBuilder<A, B, C>>,
    window_modules: WindowModules,
    timer: Box<EventTimer<Self>>,
    buffer: Vec<ArconElement<A>>,
}

impl<A, B, C, D> ComponentDefinition for EventTimeWindowAssigner<A, B, C, D>
where
    A: 'static + ArconType,
    B: 'static + Clone,
    C: 'static + ArconType,
    D: Port<Request = ArconElement<C>> + 'static + Clone,
{
    fn setup(&mut self, self_component: Arc<Component<Self>>) -> () {
        self.ctx_mut().initialise(self_component);
    }
    fn execute(&mut self, _max_events: usize, skip: usize) -> ExecuteResult {
        ExecuteResult::new(skip, skip)
    }
    fn ctx(&self) -> &ComponentContext<Self> {
        &self.ctx
    }
    fn ctx_mut(&mut self) -> &mut ComponentContext<Self> {
        &mut self.ctx
    }
    fn type_name() -> &'static str {
        "EventTimeWindowAssigner"
    }
}

impl<A, B, C, D> EventTimeWindowAssigner<A, B, C, D>
where
    A: 'static + ArconType,
    B: 'static + Clone,
    C: 'static + ArconType,
    D: Port<Request = ArconElement<C>> + 'static + Clone,
{
    pub fn new(
        partitioner: Box<Partitioner<C, D, Self>>,
        init_builder_code: String,
        udf_code: String,
        result_code: String,
        length: u64,
        slide: u64,
        late: u64,
    ) -> Self {
        let prio = 0;
        let init_builder = Arc::new(
            Module::new("init_builder".to_string(), init_builder_code, prio, None).unwrap(),
        );
        let code_module = Arc::new(Module::new("udf".to_string(), udf_code, prio, None).unwrap());
        let result_module =
            Arc::new(Module::new("result".to_string(), result_code, prio, None).unwrap());

        let window_modules = WindowModules {
            init_builder,
            udf: code_module,
            materializer: result_module,
        };

        // TODO: Should handle weld compilation errors here

        EventTimeWindowAssigner {
            ctx: ComponentContext::new(),
            partitioner,
            window_count: 0,
            window_length: length,
            window_slide: slide,
            window_start: 0,
            late_arrival_time: late,
            max_window_ts: 0,
            max_watermark: 0,
            window_map: BTreeMap::new(),
            window_modules,
            timer: Box::new(EventTimer::new()),
            buffer: Vec::new(),
        }
    }
    // Creates the next window based on window_start + window_count*window_slide
    fn new_window(&mut self) -> () {
        let ts = self.window_start + (self.window_count * self.window_slide) + self.window_length;
        debug!(self.ctx.log(), "creating new window, timestamp {}", ts);
        self.max_window_ts = ts;
        self.window_count = self.window_count + 1;
        let mut window_builder = WindowBuilder::new(self.window_modules.clone()).unwrap();
        self.window_map.insert(ts, window_builder);

        // Schedule trigger
        self.timer
            .schedule_at(ts + self.late_arrival_time, move |self_c, _| {
                let self_ptr = self_c as *const Self;
                if let Some(mut window) = self_c.window_map.remove(&ts) {
                    match window.result() {
                        Ok(e) => {
                            debug!(self_c.ctx.log(), "Window {} result materialized!", ts);
                            self_c
                                .partitioner
                                .output(ArconElement::new(e), self_ptr, Some(ts));
                        }
                        _ => {
                            error!(
                                self_c.ctx.log(),
                                "failed to get result, couldn't materialize from window_builder"
                            );
                        }
                    }
                } else {
                    error!(
                        self_c.ctx.log(),
                        "failed to get result, couldn't find the window in window_map"
                    );
                }
            });
    }
    fn new_windows_to(&mut self, ts: u64) -> () {
        if (self.window_start == 0) {
            // First time starting windows
            self.window_start = ts;
            self.timer.set_time(self.window_start);
            self.new_window();
        }
        while (self.max_window_ts - self.window_length < ts) {
            self.new_window();
        }
    }
    fn handle_element(&mut self, e: ArconElement<A>) -> () {
        let ts = e.timestamp.unwrap_or(0);
        //debug!(self.ctx.log(), "handling element with timestamp: {}", ts);

        // Always make sure we have all windows open
        self.new_windows_to(ts);

        // Insert in all relevant windows (inclusive range)
        for (t, builder) in self.window_map.range_mut(ts..=ts + self.window_length) {
            builder.on_element(e.data);
        }
    }
    fn handle_watermark(&mut self, w: &Watermark) -> () {
        //debug!(self.ctx.log(), "handling watermark with timestamp: {}", w);
        let ts = w.get_timestamp();
        // Spawn new windows if necessary
        self.new_windows_to(ts);

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

impl<A, B, C, D> Require<D> for EventTimeWindowAssigner<A, B, C, D>
where
    A: 'static + ArconType,
    B: 'static + Clone,
    C: 'static + ArconType,
    D: Port<Request = ArconElement<C>> + 'static + Clone,
{
    fn handle(&mut self, event: D::Indication) -> () {
        // ignore
    }
}

impl<A, B, C, D> Provide<ControlPort> for EventTimeWindowAssigner<A, B, C, D>
where
    A: 'static + ArconType,
    B: 'static + Clone,
    C: 'static + ArconType,
    D: Port<Request = ArconElement<C>> + 'static + Clone,
{
    fn handle(&mut self, event: ControlEvent) -> () {
        if let ControlEvent::Start = event {}
    }
}

impl<A, B, C, D> Actor for EventTimeWindowAssigner<A, B, C, D>
where
    A: 'static + ArconType,
    B: 'static + Clone,
    C: 'static + ArconType,
    D: Port<Request = ArconElement<C>> + 'static + Clone,
{
    fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
        if let Some(e) = msg.downcast_ref::<ArconElement<A>>() {
            self.handle_element(*e);
        } else if let Some(w) = msg.downcast_ref::<Watermark>() {
            self.handle_watermark(w);
        } else {
            error!(self.ctx.log(), "Unrecognized message from {:?}", _sender);
        }
    }
    fn receive_message(&mut self, _sender: ActorPath, ser_id: u64, buf: &mut Buf) {
        // This remote message receiver is untested and probably doesn't work
        if ser_id == serialisation_ids::PBUF {
            let r: Result<StreamTaskMessage, SerError> = ProtoSer::deserialise(buf);
            if let Ok(msg) = r {
                let payload = msg.payload.unwrap();
                match payload {
                    element(e) => {
                        // Send to all relevant windows
                        let ts = e.get_timestamp();
                        for (_, wp) in self.window_map.range(ts..ts + self.window_length) {}
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::partitioner::forward::*;
    use crate::streaming::partitioner::*;
    use crate::streaming::{Channel, ChannelPort};
    use kompact::default_components::DeadletterBox;
    use std::cell::RefCell;
    use std::rc::Rc;
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
        let mut cfg = KompactConfig::new();
        cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        // Create a sink
        let (sink, _) = system.create_and_register(move || sink::Sink::new());
        let sink_ref = sink.actor_ref();

        pub type Assigner =
            EventTimeWindowAssigner<u8, Appender<u8>, WindowOutput, ChannelPort<WindowOutput>>;

        let mut partitioner: Box<Forward<WindowOutput, ChannelPort<WindowOutput>, Assigner>> =
            Box::new(Forward::new(Channel::Local(sink_ref.clone())));

        // Create the window_assigner
        let builder_code = String::from("|| appender[u8]");
        let udf_code = String::from("|x: u8, y: appender[u8]| merge(y, x)");
        let udf_result = String::from("|y: appender[u8]| len(result(y))");
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
    fn timestamped_event(ts: u64) -> Box<ArconElement<u8>> {
        return Box::new(ArconElement {
            data: 1 as u8,
            timestamp: Some(ts),
        });
    }

    // Tests:
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
        let result = &sink_inspect.result[0].take().unwrap();
        assert_eq!(result, &2);
    }
    /* #[test] How should we handle late arrivals?
    fn window_late_arrival() {
        // Send 2 messages on time, a watermark and a late message which should be allowed
        let (assigner_ref, sink) = window_assigner_test_setup(10, 10, 10);
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
        let r0 = &sink_inspect.result[0].take().unwrap();
        assert_eq!(r0, &2);
        let r1 = &sink_inspect.result[1].take().unwrap();
        assert_eq!(r1, &3);
    } */
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
        let r1 = &sink_inspect.result[1].take().unwrap();
        assert_eq!(r0, &2);
        assert_eq!(r1, &0);
        assert_eq!(&sink_inspect.result.len(), &(2 as usize));
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
        let r0 = &sink_inspect.result[0].take().unwrap();
        assert_eq!(r0, &3);
        let r1 = &sink_inspect.result[1].take().unwrap();
        assert_eq!(r1, &2);
        let r2 = &sink_inspect.result[2].take().unwrap();
        assert_eq!(r2, &0);
    }
    #[test]
    fn window_fastforward() {
        // check that we receive correct number windows from fast forwarding
        let (assigner_ref, sink) = window_assigner_test_setup(5, 5, 0);
        wait(1);
        // Send messages
        let moment = now();
        assigner_ref.tell(Box::new(watermark(moment)), &assigner_ref);
        assigner_ref.tell(Box::new(watermark(moment + 26)), &assigner_ref);
        wait(1);
        // Inspect and assert
        let sink_inspect = sink.definition().lock().unwrap();
        // Should be at least 5 windows created from this sequence
        assert!(&sink_inspect.result.len() > &(5 as usize));
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
        let r0 = &sink_inspect.result[0].take().unwrap();
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
            EventTimeWindowAssigner<u8, Appender<u8>, WindowOutput, ChannelPort<WindowOutput>>;
        let mut req_port: RequiredPort<ChannelPort<WindowOutput>, Assigner> = RequiredPort::new();
        let _ = req_port.connect(target_port);
        let ref_port = crate::streaming::RequirePortRef(Rc::new(RefCell::new(req_port)));
        let comp_channel: Channel<WindowOutput, ChannelPort<WindowOutput>, Assigner> =
            Channel::Port(ref_port);

        let mut partitioner: Box<Forward<WindowOutput, ChannelPort<WindowOutput>, Assigner>> =
            Box::new(Forward::new(Channel::Local(sink.actor_ref().clone())));

        // Create the window_assigner
        let builder_code = String::from("|| appender[u8]");
        let udf_code = String::from("|x: u8, y: appender[u8]| merge(y, x)");
        let udf_result = String::from("|y: appender[u8]| len(result(y))");
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
