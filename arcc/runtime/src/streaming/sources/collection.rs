use kompact::*;
//use std::collections;
use std::sync::Arc;
/*
    CollectionSource:
    Allows generation of events from a rust Collection.
    Collection and a subscriber for where to send events.
    Each instance in the collection will be sent as an event.
*/

pub struct CollectionSource<A: 'static + Send + Sync + Clone> {
    ctx: ComponentContext<CollectionSource<A>>,
    subscriber: Arc<ActorRef>,
    collection: Vec<A>,
}

impl<A: Send + Sync + Clone> CollectionSource<A> {
    pub fn new(collection: Vec<A>, subscriber: ActorRef) -> CollectionSource<A> {
        CollectionSource {
            ctx: ComponentContext::new(),
            subscriber: Arc::new(subscriber),
            collection: collection,
        }
    }
    pub fn process_collection(&self) {
        let actor_ref = self.actor_ref();
        for element in &self.collection {
            self.subscriber.tell(Arc::new(element.clone()), &actor_ref);
        }
        /*
        if let Ok(f) = File::open(&self.file_path) {
            let reader = BufReader::new(f);

            for line in reader.lines() {
                match line {
                    Ok(l) => {
                        if let Ok(v) = l.parse::<A>() {
                            self.subscriber.tell(Box::new(v), &self.actor_ref());
                        }
                    }
                    _ => {
                        error!(self.ctx.log(), "Unable to read line {}", self.file_path);
                    }
                }
            }
        } else {
            error!(self.ctx.log(), "Unable to open file {}", self.file_path);
        }
        */
    }
}

impl<A: Send + Sync + Clone> Provide<ControlPort> for CollectionSource<A> {
    fn handle(&mut self, event: ControlEvent) -> () {
        match event {
            ControlEvent::Start => {
                self.process_collection();
            }
            _ => {
                error!(self.ctx.log(), "bad ControlEvent");
            }
        }
    }
}

impl<A: Send + Sync + Clone> ComponentDefinition for CollectionSource<A> {
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
        "CollectionSource"
    }
}

impl<A: Send + Sync + Clone> Actor for CollectionSource<A> {
    fn receive_local(&mut self, _sender: ActorRef, _msg: &Any) {}

    fn receive_message(&mut self, _sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use kompact::default_components::DeadletterBox;
    use std::io::prelude::*;
    use std::{fs, thread, time};

    // Stub for window-results
    mod sink {
        use super::*;

        pub struct Sink<A: 'static + Send + Clone> {
            ctx: ComponentContext<Sink<A>>,
            pub result: Vec<A>,
        }
        impl<A: Send + Clone> Sink<A> {
            pub fn new(t: A) -> Sink<A> {
                Sink {
                    ctx: ComponentContext::new(),
                    result: Vec::new(),
                }
            }
        }
        impl<A: Send + Clone> Provide<ControlPort> for Sink<A> {
            fn handle(&mut self, _event: ControlEvent) -> () {}
        }
        impl<A: Send + Clone> Actor for Sink<A> {
            fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
                println!("sink received message");
                if let Some(m) = msg.downcast_ref::<A>() {
                    println!("trying to push");
                    self.result.push((*m).clone());
                } else {
                    println!("unrecognized message");
                }
            }
            fn receive_message(&mut self, _sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {}
        }
        impl<A: Send + Clone> ComponentDefinition for Sink<A> {
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
    }
    // Shared methods for test cases
    fn wait(time: u64) -> () {
        thread::sleep(time::Duration::from_secs(time));
    }
    fn test_setup<A: Send + Clone>(
        a: A,
    ) -> (
        kompact::KompactSystem,
        Arc<kompact::Component<sink::Sink<A>>>,
    ) {
        // Kompact set-up
        let mut cfg = KompactConfig::new();
        cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let (sink, _) = system.create_and_register(move || sink::Sink::new(a));

        system.start(&sink);

        return (system, sink);
    }
    // Test cases
    #[test]
    fn collection_strings() {
        let (system, sink) = test_setup(String::new());
        let mut collection = Vec::new();
        collection.push("hej".to_string());
        collection.push("hello".to_string());

        let file_source: CollectionSource<String> =
            CollectionSource::new(collection, sink.actor_ref());
        let (source, _) = system.create_and_register(move || file_source);
        system.start(&source);
        wait(1);

        let sink_inspect = sink.definition().lock().unwrap();
        let r0 = &sink_inspect.result[0];
        let r1 = &sink_inspect.result[1];
        assert_eq!(&sink_inspect.result.len(), &(2 as usize));
        assert_eq!(r0, "hej");
        assert_eq!(r1, "hello");
    }
    #[test]
    fn collection_f32() {
        let (system, sink) = test_setup(1 as f32);
        let mut collection = Vec::new();
        collection.push(123 as f32);
        collection.push(321.9 as f32);

        let file_source: CollectionSource<f32> =
            CollectionSource::new(collection, sink.actor_ref());
        let (source, _) = system.create_and_register(move || file_source);
        system.start(&source);
        wait(1);

        let sink_inspect = sink.definition().lock().unwrap();
        let r0 = &sink_inspect.result[0];
        let r1 = &sink_inspect.result[1];
        assert_eq!(&sink_inspect.result.len(), &(2 as usize));
        assert_eq!(*r0, 123 as f32);
        assert_eq!(*r1, 321.9 as f32);
    }
}
