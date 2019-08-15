use crate::data::{ArconElement, ArconEvent, ArconType, Watermark};
use crate::error::ArconResult;
use crate::streaming::channel::strategy::*;
use crate::streaming::window::builder::{WindowBuilder, WindowFn, WindowModules};
use crate::weld::module::Module;
use arcon_macros::arcon_task;
use kompact::*;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use std::convert::TryInto;


/*
    ProcessingTimeWindowAssigner
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
#[arcon_task]
#[derive(ComponentDefinition)]
pub struct ProcessingTimeWindowAssigner<IN, FUNC, OUT>
where
    IN: 'static + ArconType + Hash,
    FUNC: 'static + Clone,
    OUT: 'static + ArconType,
{
    ctx: ComponentContext<Self>,
    channel_strategy: Box<ChannelStrategy<OUT>>,
    window_length: u128,
    window_slide: u128,
    late_arrival_time: u128,
    window_start: HashMap<u64, u128>,
    window_maps: HashMap<u64, HashMap<u128, WindowBuilder<IN, FUNC, OUT>>>,
    window_modules: WindowModules,
    hasher: BuildHasherDefault<DefaultHasher>,
    keyed: bool,
}

impl<IN, FUNC, OUT> ProcessingTimeWindowAssigner<IN, FUNC, OUT>
where
    IN: 'static + ArconType + Hash,
    FUNC: 'static + Clone,
    OUT: 'static + ArconType,
{
    pub fn new(
        channel_strategy: Box<ChannelStrategy<OUT>>,
        init_builder_code: String,
        udf_code: String,
        result_code: String,
        length: u128,
        slide: u128,
        late: u128,
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

        ProcessingTimeWindowAssigner {
            ctx: ComponentContext::new(),
            channel_strategy,
            window_length: length,
            window_slide: slide,
            late_arrival_time: late,
            window_start: HashMap::new(),
            window_maps: HashMap::new(),
            window_modules,
            hasher: BuildHasherDefault::<DefaultHasher>::default(),
            keyed,
        }
    }

    // Creates the window trigger for a key and "window index"
    fn new_window_trigger(&mut self, key: u64, index: u128) -> () {
        let w_start = match self.window_start.get(&key) {
            Some(start) => start,
            None => {
                panic!("tried to schedule window_trigger for key which hasn't started");
            }
        };
        let mut now = 0;
        let window_close = w_start + (index * self.window_slide) + self.window_length + self.late_arrival_time;
        match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(timestamp) => {
                now = timestamp.as_millis();
            }
            _ => {
                error!(self.ctx.log(), "unable to get current time while scheduling trigger");
            }
        }
        
        let timeout: u64 = (window_close - now).try_into().unwrap();
        debug!(
            self.ctx.log(),
            "creating new window for key {}, timestamp {}", key, &window_close
        );
        
        // Schedule trigger
        self.schedule_once(Duration::from_millis(timeout.try_into().unwrap()),
            move |self_c, _| {
                if let Some(w_map) = self_c.window_maps.get_mut(&key) {
                    match w_map.remove(&index) {
                        Some(mut window) => {
                            match window.result() {
                                Ok(e) => {
                                    debug!(self_c.ctx.log(), "Window {} result materialized!", window_close);
                                    if let Err(_err) = self_c
                                        .channel_strategy
                                        .output(ArconEvent::Element(ArconElement::new(e)), &self_c.ctx.system()) {
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
                            error!(self_c.ctx.log(), "No window found for key {} and timestamp {}", key, window_close);
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
        if !self.keyed {
            return 0;
        }
        let mut h = self.hasher.build_hasher();
        e.data.hash(&mut h);
        return h.finish();
    }
    fn handle_element(&mut self, e: &ArconElement<IN>) -> ArconResult<()> {
        let ts: u128;
        match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(timestamp) => {
                ts = timestamp.as_millis();
            }
            _ => {
                return arcon_err!("Could not read SystemTime");
            }
        }

        let key = self.get_key(*e);
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
        Ok(())
    }
    fn handle_watermark(&mut self, _w: Watermark) -> ArconResult<()> {
        // does nothing...
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    use crate::streaming::channel::strategy::forward::*;
    use crate::streaming::channel::Channel;
    use std::{thread, time};
    use weld::data::Appender;

    // Stub for window-results
    mod sink {
        use super::*;

        #[derive(ComponentDefinition)]
        pub struct Sink {
            ctx: ComponentContext<Sink>,
            pub result: Vec<Option<i64>>,
            pub watermarks: Vec<Option<u64>>,
        }
        impl Sink {
            pub fn new() -> Sink {
                Sink {
                    ctx: ComponentContext::new(),
                    result: Vec::new(),
                    watermarks: Vec::new(),
                }
            }
        }
        impl Provide<ControlPort> for Sink {
            fn handle(&mut self, _event: ControlEvent) -> () {}
        }
        impl Actor for Sink {
            fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
                if let Some(event) = msg.downcast_ref::<ArconEvent<WindowOutput>>() {
                    match event {
                        ArconEvent::Element(e) => {
                            self.result.push(Some(e.data.len));
                        }
                        ArconEvent::Watermark(w) => {
                            self.watermarks.push(Some(w.timestamp));
                        }
                    }
                }
            }
            fn receive_message(&mut self, _sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {}
        }
    }

    #[arcon]
    #[derive(Hash)]
    pub struct WindowOutput {
        pub len: i64,
    }

    #[key_by(id)]
    #[arcon]
    pub struct Item {
        id: u64,
        price: u32,
    }

    // helper functions
    fn keyed_event(id: u64) -> Box<ArconEvent<Item>> {
        return Box::new(ArconEvent::Element(ArconElement {
            data: Item { id, price: 1 },
            timestamp: None,
        }));
    }
    fn wait(time: u64) -> () {
        thread::sleep(time::Duration::from_secs(time));
    }
    fn window_assigner_test_setup(
        length: u128,
        slide: u128,
        late: u128,
    ) -> (ActorRef, Arc<kompact::Component<sink::Sink>>) {
        // Kompact set-up
        let cfg = KompactConfig::new();
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        // Create a sink
        let (sink, _) = system.create_and_register(move || sink::Sink::new());
        let sink_ref = sink.actor_ref();

        let channel_strategy: Box<Forward<WindowOutput>> =
            Box::new(Forward::new(Channel::Local(sink_ref.clone())));

        // Create the window_assigner
        let builder_code = String::from("|| appender[u32]");
        let udf_code = String::from("|x: {u64, u32}, y: appender[u32]| merge(y, x.$1)");
        let udf_result = String::from("|y: appender[u32]| len(result(y))");
        let window_assigner = ProcessingTimeWindowAssigner::<Item, Appender<u32>, WindowOutput>::new(
            channel_strategy,
            builder_code,
            udf_code,
            udf_result,
            length,
            slide,
            late,
            true,
        );

        let (assigner, _) = system.create_and_register(move || window_assigner);
        let win_ref = assigner.actor_ref();
        system.start(&sink);
        system.start(&assigner);
        return (win_ref, sink);
    }

    #[test]
    fn processing_time_window_overlapping() {
        // Use overlapping windows (slide = length/2), check that messages appear correctly
        let (assigner_ref, sink) = window_assigner_test_setup(6000, 3000, 0);
        wait(1);
        // Send messages
        assigner_ref.tell(keyed_event(1), &assigner_ref); // 1st window opens
        wait(4);
        assigner_ref.tell(keyed_event(1), &assigner_ref); // 2nd window opens, element goes into both
        assigner_ref.tell(keyed_event(2), &assigner_ref); // 1st window opens for different key
        wait(7); // wait for all three windows to trigger
        
        // Inspect and assert
        let mut sink_inspect = sink.definition().lock().unwrap();
        let rl = &sink_inspect.result.len();
        assert_eq!(rl, &3); // 3 windows triggered
        let r0 = &sink_inspect.result[0].take().unwrap();
        assert_eq!(r0, &2); // 1st window 2 elements
        let r1 = &sink_inspect.result[1].take().unwrap();
        assert_eq!(r1, &1); // 2nd window 1 element
        let r2 = &sink_inspect.result[2].take().unwrap();
        assert_eq!(r2, &1); // 3rd window 1 element
    }
}