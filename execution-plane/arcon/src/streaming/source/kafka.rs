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
#[derive(ComponentDefinition)]
pub struct KafkaSource<A: 'static + ArconType> {
    ctx: ComponentContext<KafkaSource<A>>,
    target_pointer: ActorRef,
    checkpoint_interval: u64,
    _output_format: A,
}

impl<A: ArconType> Provide<ControlPort> for KafkaSource<A> {
    fn handle(&mut self, event: ControlEvent) -> () {}
}
impl<A: ArconType> Actor for KafkaSource<A> {
    fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {}

    fn receive_message(&mut self, _sender: ActorPath, ser_id: u64, buf: &mut Buf) {}
}

impl<A: ArconType> KafkaSource<A> {}
