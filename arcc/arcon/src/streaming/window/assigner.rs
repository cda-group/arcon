use crate::data::{ArconElement, ArconType};
use crate::streaming::partitioner::*;
use crate::streaming::window::builder::WindowModules;
use crate::streaming::window::component;
use crate::weld::module::Module;
use kompact::*;
use crate::messages::protobuf::StreamTaskMessage_oneof_payload::*;
use crate::messages::protobuf::*;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;
use std::time;

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
#[derive(ComponentDefinition)]
pub struct EventTimeWindowAssigner<A, B, C, D>
where
    A: 'static + ArconType,
    B: 'static + Clone,
    C: 'static + ArconType,
    D: Port<Request = ArconElement<C>> + 'static + Clone,
{
    ctx: ComponentContext<Self>,
    partitioner: Arc<Mutex<Partitioner<C, D, component::WindowComponent<A, B, C, D>>>>,
    window_count: u64,
    window_length: u64,
    window_slide: u64,
    window_start: u64,
    late_arrival_time: u64,
    max_window_ts: u64,
    max_watermark: u64,
    window_map: BTreeMap<u64, ActorRef>,
    component_map: BTreeMap<u64, Arc<Component<component::WindowComponent<A, B, C, D>>>>,
    window_modules: WindowModules,
}

impl<A, B, C, D> EventTimeWindowAssigner<A, B, C, D>
where
    A: 'static + ArconType,
    B: 'static + Clone,
    C: 'static + ArconType,
    D: Port<Request = ArconElement<C>> + 'static + Clone,
{
    pub fn new(
        partitioner: Arc<Mutex<Partitioner<C, D, component::WindowComponent<A, B, C, D>>>>,
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
            component_map: BTreeMap::new(), // Used for direct access to components, for killing
            window_modules,
        }
    }
    // Creates the next window based on window_start + window_count*window_slide
    fn new_window(&mut self) -> () {
        let ts = self.window_start + (self.window_count * self.window_slide) + self.window_length;
        let wc = component::WindowComponent::new(
            self.partitioner.clone(),
            self.window_modules.clone(),
            ts.clone(),
        );
        let (wp, _) = self.ctx.system().create_and_register(move || wc);
        self.ctx.system().start(&wp);
        self.window_map.insert(ts, wp.actor_ref());
        self.component_map.insert(ts, wp);
        self.max_window_ts = ts;
        self.window_count = self.window_count + 1;
    }
    fn handle_watermark(&mut self, w: u64) -> () {
        // Only deal with watermark if it's a higher watermark than we've previously seen
        if w > self.max_watermark {
            self.max_watermark = w;
        } else {
            return;
        }

        // Spawn new windows if we are behind, "fastforward"
        while self.max_window_ts < w + self.window_length {
            debug!(
                self.ctx.log(),
                "WindowAssigner received a future watermark {} fast-forwarding window creation", w
            );
            self.new_window();
        }

        // Check for windows to trigger
        for (_, wp) in self.window_map.range(0..w) {
            wp.tell(Box::new(create_window_trigger()), &self.actor_ref());
        }

        // Check for windows to kill
        let new_map = self.window_map.split_off(&(w - self.late_arrival_time));
        for (ts, _) in &self.window_map {
            if let Some(comp) = self.component_map.remove(ts) {
                debug!(self.ctx.log(), "WindowAssigner killing window {}", ts);
                self.ctx.system().kill(comp);
            }
        }

        // Update the window map
        self.window_map = new_map;
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
        if let ControlEvent::Start = event {
            // Register windowing start time
            self.window_start = time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("error")
                .as_secs();
            debug!(
                self.ctx.log(),
                "WindowAssigner starting at time {}", self.window_start
            );
            // Create two windows to cover early arrivals and set-up time for the components
            self.new_window();
            self.new_window();

            // Schedule regular creation of new windows
            self.schedule_periodic(
                time::Duration::from_secs(self.window_slide),
                time::Duration::from_secs(self.window_slide),
                |self_c, _| {
                    self_c.new_window();
                },
            );
        }
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
        if let Some(payload) = msg.downcast_ref::<ArconElement<A>>() {
            // Send to all relevant windows
            let ts = payload.timestamp.unwrap_or(0);
            for (_, wp) in self.window_map.range(ts..ts + self.window_length) {
                wp.tell(Box::new(payload.clone()), &self.actor_ref());
            }
        } else if let Some(w) = msg.downcast_ref::<Watermark>() {
            self.handle_watermark(w.get_timestamp());
        } else {
            error!(self.ctx.log(), "Unrecognized message from {:?}", _sender);
        }
    }
    fn receive_message(&mut self, _sender: ActorPath, ser_id: u64, buf: &mut Buf) {
        if ser_id == serialisation_ids::PBUF {
            let r: Result<StreamTaskMessage, SerError> = ProtoSer::deserialise(buf);
            if let Ok(msg) = r {
                let payload = msg.payload.unwrap();
                match payload {
                    element(e) => {
                        // Send to all relevant windows
                        let ts = e.get_timestamp();
                        for (_, wp) in self.window_map.range(ts..ts + self.window_length) {
                            wp.tell(Box::new(e.clone()), &self.actor_ref());
                        }
                    }
                    keyed_element(_) => {}
                    watermark(w) => {
                        self.handle_watermark(w.get_timestamp());
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
    use crate::streaming::{Channel, ChannelPort};
    use kompact::default_components::DeadletterBox;
    use std::cell::UnsafeCell;
    use std::rc::Rc;
    use weld::data::Appender;

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
            fn handle(&mut self, _event: ()) -> () {
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

        pub type WindowC =
            component::WindowComponent<u8, Appender<u8>, WindowOutput, ChannelPort<WindowOutput>>;

        let partitioner: Arc<Mutex<Forward<WindowOutput, ChannelPort<WindowOutput>, WindowC>>> =
            Arc::new(Mutex::new(Forward::new(Channel::Local(sink_ref.clone()))));

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
        std::thread::sleep(time::Duration::from_secs(time));
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
    fn discard_late_arrival() {
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
    #[test]
    fn late_arrival() {
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
    }
    #[test]
    fn too_late_late_arrival() {
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
    fn overlapping_windows() {
        // Use overlapping windows (slide = length/2), check that messages appear correctly
        let (assigner_ref, sink) = window_assigner_test_setup(10, 5, 2);
        wait(1);
        // Send messages
        let moment = now();
        assigner_ref.tell(timestamped_event(moment), &assigner_ref);
        assigner_ref.tell(timestamped_event(moment + 6), &assigner_ref);
        assigner_ref.tell(timestamped_event(moment + 6), &assigner_ref);
        assigner_ref.tell(Box::new(watermark(moment + 11)), &assigner_ref);
        wait(1);
        assigner_ref.tell(Box::new(watermark(moment + 21)), &assigner_ref);
        wait(1);
        // Inspect and assert
        let mut sink_inspect = sink.definition().lock().unwrap();
        let r0 = &sink_inspect.result[0].take().unwrap();
        let r1 = &sink_inspect.result[1].take().unwrap();
        let r2 = &sink_inspect.result[2].take().unwrap();
        assert_eq!(r0, &3);
        assert_eq!(r1, &2);
        assert_eq!(r2, &0);
    }
    #[test]
    fn fastforward_windows() {
        // check that we receive correct number windows from fast forwarding
        let (assigner_ref, sink) = window_assigner_test_setup(5, 5, 0);
        wait(1);
        // Send messages
        let moment = now();
        assigner_ref.tell(Box::new(watermark(moment + 25)), &assigner_ref);
        wait(1);
        // Inspect and assert
        let sink_inspect = sink.definition().lock().unwrap();
        assert_eq!(&sink_inspect.result.len(), &(5 as usize));
    }
    #[test]
    fn empty_window() {
        // check that we receive correct number windows from fast forwarding
        let (assigner_ref, sink) = window_assigner_test_setup(5, 5, 0);
        wait(1);
        assigner_ref.tell(Box::new(watermark(now() + 5)), &assigner_ref);
        wait(1);
        let mut sink_inspect = sink.definition().lock().unwrap();
        let r0 = &sink_inspect.result[0].take().unwrap();
        assert_eq!(r0, &0);
        assert_eq!(&sink_inspect.result.len(), &(1 as usize));
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

        pub type WindowC =
            component::WindowComponent<u8, Appender<u8>, WindowOutput, ChannelPort<WindowOutput>>;

        // Create a sink and create port channel
        let (sink, _) = system.create_and_register(move || sink::Sink::new());
        let target_port = sink.on_definition(|c| c.sink_port.share());
        let mut req_port: RequiredPort<ChannelPort<WindowOutput>, WindowC> = RequiredPort::new();
        let _ = req_port.connect(target_port);
        let ref_port = crate::streaming::RequirePortRef(Rc::new(UnsafeCell::new(req_port)));
        let comp_channel: Channel<WindowOutput, ChannelPort<WindowOutput>, WindowC> =
            Channel::Port(ref_port);

        // Define partitioner
        let partitioner: Arc<Mutex<Forward<WindowOutput, ChannelPort<WindowOutput>, WindowC>>> =
            Arc::new(Mutex::new(Forward::new(comp_channel)));

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
