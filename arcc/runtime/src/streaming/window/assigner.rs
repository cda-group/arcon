use crate::streaming::window::builder::WindowModules;
use crate::streaming::window::component;
use crate::weld::module::Module;
use kompact::*;
use messages::protobuf::StreamTaskMessage_oneof_payload::*;
use messages::protobuf::*;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::sync::Arc;
use std::time::UNIX_EPOCH;
use std::{thread, time};
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

// Used to avoid serialization/deserialization of data in local sends
#[derive(Clone, Debug)]
pub struct LocalElement<T> {
    pub data: T,
    pub timestamp: u64,
}

pub struct EventTimeWindowAssigner<
    A: 'static + Send + Clone + Sync + Debug + Display,
    B: 'static + Clone,
    C: 'static + Send + Clone + Sync + Display,
> {
    ctx: ComponentContext<EventTimeWindowAssigner<A, B, C>>,
    target_pointer: ActorRef,
    window_count: u64,
    window_length: u64,
    window_slide: u64,
    window_start: u64,
    late_arrival_time: u64,
    max_window_ts: u64,
    max_watermark: u64,
    window_map: BTreeMap<u64, ActorRef>,
    component_map: BTreeMap<u64, Arc<Component<component::WindowComponent<A, B, C>>>>,
    window_module: WindowModules,
}

// Implement ComponentDefinition
impl<A: Send + Clone + Sync + Debug + Display, B: Clone, C: Send + Clone + Sync + Display>
    ComponentDefinition for EventTimeWindowAssigner<A, B, C>
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

impl<A: Send + Clone + Sync + Debug + Display, B: Clone, C: Send + Clone + Sync + Display>
    EventTimeWindowAssigner<A, B, C>
{
    pub fn new(
        target: ActorRef,
        init_builder_code: String,
        udf_code: String,
        result_code: String,
        length: u64,
        slide: u64,
        late: u64,
    ) -> EventTimeWindowAssigner<A, B, C> {
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
            target_pointer: target.clone(),
            window_count: 0,
            window_length: length,
            window_slide: slide,
            window_start: 0,
            late_arrival_time: late,
            max_window_ts: 0,
            max_watermark: 0,
            window_map: BTreeMap::new(),
            component_map: BTreeMap::new(), // Used for direct access to components, for killing
            window_module: window_modules,
        }
    }
    // Creates the next window based on window_start + window_count*window_slide
    fn new_window(&mut self) -> () {
        // Clone variables so they can spawn new component
        let target = self.target_pointer.clone();
        //let new_window_module = self.window_module.clone();
        let ts = self.window_start + (self.window_count * self.window_slide) + self.window_length;

        let wc: component::WindowComponent<A, B, C> =
            component::WindowComponent::new(target, self.window_module.clone(), ts.clone());
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

impl<A: Send + Clone + Sync + Debug + Display, B: Clone, C: Send + Clone + Sync + Display>
    Provide<ControlPort> for EventTimeWindowAssigner<A, B, C>
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

impl<A: Send + Clone + Sync + Debug + Display, B: Clone, C: Send + Clone + Sync + Display> Actor
    for EventTimeWindowAssigner<A, B, C>
{
    fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
        if let Some(payload) = msg.downcast_ref::<LocalElement<A>>() {
            // Send to all relevant windows
            let ts = payload.timestamp;
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
    use kompact::default_components::DeadletterBox;
    use weld::data::Appender;
    use weld::data::WeldVec;

    // Stub for window-results
    mod sink {
        use super::*;

        #[derive(ComponentDefinition)]
        pub struct Sink {
            ctx: ComponentContext<Sink>,
            pub result: Vec<Option<WeldVec<u8>>>,
        }
        impl Sink {
            pub fn new() -> Sink {
                Sink {
                    ctx: ComponentContext::new(),
                    result: Vec::new(),
                }
            }
        }
        impl Provide<ControlPort> for Sink {
            fn handle(&mut self, _event: ControlEvent) -> () {}
        }
        impl Actor for Sink {
            fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
                println!("sink received message");
                if let Some(m) = msg.downcast_ref::<LocalElement<WeldVec<u8>>>() {
                    self.result.push(Some(m.data.clone()));
                }
            }
            fn receive_message(&mut self, _sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {}
        }
    }

    // helper functions
    fn test_setup(
        length: u64,
        slide: u64,
        late: u64,
    ) -> (
        Arc<kompact::Component<EventTimeWindowAssigner<u8, Appender<u8>, WeldVec<u8>>>>,
        ActorRef,
        Arc<kompact::Component<sink::Sink>>,
    ) {
        // Kompact set-up
        let mut cfg = KompactConfig::new();
        cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        // Create a sink
        let (sink, _) = system.create_and_register(move || sink::Sink::new());
        let sink_ref = sink.actor_ref();

        // Create the window_assigner
        let builder_code = String::from("|| appender[u8]");
        let udf_code = String::from("|x: u8, y: appender[u8]| merge(y, x)");
        let udf_result = String::from("|y: appender[u8]| result(y)");
        let window_assigner: EventTimeWindowAssigner<u8, Appender<u8>, WeldVec<u8>> =
            EventTimeWindowAssigner::new(
                sink_ref,
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
        return (assigner, win_ref, sink);
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
    fn timestamped_event(ts: u64) -> Box<LocalElement<u8>> {
        return Box::new(LocalElement {
            data: 1 as u8,
            timestamp: ts,
        });
    }

    // Tests:
    #[test]
    fn discard_late_arrival() {
        // Send 2 messages on time, a watermark and a late arrival which is not allowed
        let (_, assigner_ref, sink) = test_setup(10, 10, 0);
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
        assert_eq!(result.len, 2);
    }
    #[test]
    fn late_arrival() {
        // Send 2 messages on time, a watermark and a late message which should be allowed
        let (_, assigner_ref, sink) = test_setup(10, 10, 10);
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
        assert_eq!(r0.len, 2);
        let r1 = &sink_inspect.result[1].take().unwrap();
        assert_eq!(r1.len, 3);
    }
    #[test]
    fn too_late_late_arrival() {
        // Send 2 messages on time, and then 1 message which is too late
        let (_, assigner_ref, sink) = test_setup(10, 10, 10);
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
        assert_eq!(r0.len, 2);
        assert_eq!(r1.len, 0);
        assert_eq!(&sink_inspect.result.len(), &(2 as usize));
    }
    #[test]
    fn overlapping_windows() {
        // Use overlapping windows (slide = length/2), check that messages appear correctly
        let (_, assigner_ref, sink) = test_setup(10, 5, 2);
        wait(1);
        // Send messages
        let moment = now();
        assigner_ref.tell(timestamped_event(moment), &assigner_ref);
        assigner_ref.tell(timestamped_event(moment + 6), &assigner_ref);
        assigner_ref.tell(timestamped_event(moment + 6), &assigner_ref);
        // assigner_ref.tell(Box::new(watermark(moment + 11)), &assigner_ref);
        wait(1);
        assigner_ref.tell(Box::new(watermark(moment + 21)), &assigner_ref);
        wait(1);
        // Inspect and assert
        let mut sink_inspect = sink.definition().lock().unwrap();
        let r0 = &sink_inspect.result[0].take().unwrap();
        let r1 = &sink_inspect.result[1].take().unwrap();
        let r2 = &sink_inspect.result[2].take().unwrap();
        assert_eq!(r0.len, 3);
        assert_eq!(r1.len, 2);
        assert_eq!(r2.len, 0);
    }
    #[test]
    fn fastforward_windows() {
        // check that we receive correct number windows from fast forwarding
        let (_, assigner_ref, sink) = test_setup(5, 5, 0);
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
        let (_, assigner_ref, sink) = test_setup(5, 5, 0);
        wait(1);
        assigner_ref.tell(Box::new(watermark(now() + 5)), &assigner_ref);
        wait(1);
        let mut sink_inspect = sink.definition().lock().unwrap();
        let r0 = &sink_inspect.result[0].take().unwrap();
        assert_eq!(r0.len, 0);
        assert_eq!(&sink_inspect.result.len(), &(1 as usize));
    }
}
