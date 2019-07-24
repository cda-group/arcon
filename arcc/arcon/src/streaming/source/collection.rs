use crate::data::ArconType;
use kompact::*;
use std::sync::Arc;
/*
    CollectionSource:
    Allows generation of events from a rust Vec.
    Parameters: The Vec from which it will create events and a subscriber for where to send events.
    Each instance in the collection will be sent as an event.
*/
#[derive(ComponentDefinition)]
pub struct CollectionSource<A: 'static + ArconType> {
    ctx: ComponentContext<CollectionSource<A>>,
    subscriber: Arc<ActorRef>,
    collection: Vec<A>,
}

impl<A: ArconType> CollectionSource<A> {
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
    }
}

impl<A: ArconType> Provide<ControlPort> for CollectionSource<A> {
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

impl<A: ArconType> Actor for CollectionSource<A> {
    fn receive_local(&mut self, _sender: ActorRef, _msg: &Any) {}

    fn receive_message(&mut self, _sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use kompact::default_components::DeadletterBox;
    use std::{thread, time};

    // Stub for window-results
    mod sink {
        use super::*;

        pub struct Sink<A: 'static + ArconType> {
            ctx: ComponentContext<Sink<A>>,
            pub result: Vec<A>,
        }
        impl<A: ArconType> Sink<A> {
            pub fn new(_t: A) -> Sink<A> {
                Sink {
                    ctx: ComponentContext::new(),
                    result: Vec::new(),
                }
            }
        }
        impl<A: ArconType> Provide<ControlPort> for Sink<A> {
            fn handle(&mut self, _event: ControlEvent) -> () {}
        }
        impl<A: ArconType> Actor for Sink<A> {
            fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
                if let Some(m) = msg.downcast_ref::<A>() {
                    self.result.push((*m).clone());
                } else {
                    println!("unrecognized message");
                }
            }
            fn receive_message(&mut self, _sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {}
        }
        impl<A: ArconType> ComponentDefinition for Sink<A> {
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
    fn test_setup<A: ArconType>(
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
        let r0 = sink_inspect.result[0];
        let r1 = sink_inspect.result[1];
        assert_eq!(sink_inspect.result.len(), (2 as usize));
        assert_eq!(r0, 123 as f32);
        assert_eq!(r1, 321.9 as f32);
    } /* Need to change ArconType
      #[test]
      fn collection_tuple() {
          let (system, sink) = test_setup((1 as f32, 1 as u8));
          let mut collection = Vec::new();
          collection.push((123 as f32, 2 as u8));
          collection.push((123.33 as f32, 3 as u8));

          let file_source: CollectionSource<(f32, u8)> =
              CollectionSource::new(collection, sink.actor_ref());
          let (source, _) = system.create_and_register(move || file_source);
          system.start(&source);
          wait(1);

          let sink_inspect = sink.definition().lock().unwrap();
          let r0 = sink_inspect.result[0];
          let r1 = sink_inspect.result[1];
          assert_eq!(sink_inspect.result.len(), (2 as usize));
          assert_eq!(r0, (123 as f32, 2 as u8));
          assert_eq!(r1, (123.33 as f32, 3 as u8));
      } */
}
