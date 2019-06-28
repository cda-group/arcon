use kompact::*;
use std::sync::Arc;
/*
    * * * Not implemented! * * *

    KafkaSource:
    Allows generation of events from a file.
    Takes file path and parameters for how to parse it and target for where to send events.

    TODO:
        Struct and new() with parameters
        Read kafka
        Event generation
        Watermark generation

    Flink Structure
        KafkaConsumerThread essentially does:
            records = KafkaConsumer.poll();
            handover.produce(records);
        In an iterated loop, checking partitions etc. between every iteration.


*/

pub struct KafkaSource<A: 'static + Send> {
    ctx: ComponentContext<KafkaSource<A>>,
    target_pointer: ActorRef,
    checkpoint_interval: u64,
    _output_format: A,
}

impl<A: Send> ComponentDefinition for KafkaSource<A> {
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
        "KafkaSource"
    }
}

impl<A: Send> Provide<ControlPort> for KafkaSource<A> {
    fn handle(&mut self, event: ControlEvent) -> () {}
}
impl<A: Send> Actor for KafkaSource<A> {
    fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {}

    fn receive_message(&mut self, _sender: ActorPath, ser_id: u64, buf: &mut Buf) {}
}

impl<A: Send> KafkaSource<A> {}
