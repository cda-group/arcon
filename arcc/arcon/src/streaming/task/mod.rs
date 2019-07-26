pub mod filter;
pub mod flatmap;
pub mod manager;
pub mod map;
pub mod stateless;

use crate::data::{ArconElement, ArconType};
use crate::error::*;
use crate::messages::protobuf::StreamTaskMessage;
use crate::messages::protobuf::StreamTaskMessage_oneof_payload::*;

pub struct TaskMetric {
    avg: u64,
    executions: u64,
}

impl TaskMetric {
    pub fn new() -> Self {
        TaskMetric {
            avg: 0,
            executions: 0,
        }
    }
    pub fn update_avg(&mut self, ns: u64) {
        if self.executions == 0 {
            self.avg = ns;
        } else {
            let ema: i32 = (ns as i32 - self.avg as i32) * (2 / (self.executions + 1)) as i32
                + self.avg as i32;
            self.avg = ema as u64;
        }
        self.executions += 1;
    }

    pub fn get_avg(&self) -> u64 {
        self.avg
    }
}

// TODO: Change ArconElement return type to ArconEvent...
pub fn get_remote_msg<A: ArconType>(data: StreamTaskMessage) -> ArconResult<ArconElement<A>> {
    let payload = data.payload.unwrap();

    let msg = match payload {
        element(e) => {
            let event: A = bincode::deserialize(e.get_data()).map_err(|e| {
                arcon_err_kind!("Failed to deserialise event with err {}", e.to_string())
            })?;
            Ok(ArconElement::with_timestamp(event, e.get_timestamp()))
        }
        keyed_element(_) => {
            unimplemented!();
        }
        watermark(_) => {
            unimplemented!();
        }
        checkpoint(_) => {
            unimplemented!();
        }
    };

    msg
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::channel::ChannelPort;
    use kompact::*;

    /// A component that is used during testing of Tasks..
    #[derive(ComponentDefinition)]
    #[allow(dead_code)]
    pub struct TaskSink<A>
    where
        A: 'static + ArconType,
    {
        ctx: ComponentContext<Self>,
        in_port: ProvidedPort<ChannelPort<A>, Self>,
        pub result: Vec<A>,
    }

    impl<A> TaskSink<A>
    where
        A: 'static + ArconType,
    {
        pub fn new() -> Self {
            TaskSink {
                ctx: ComponentContext::new(),
                in_port: ProvidedPort::new(),
                result: Vec::new(),
            }
        }
    }

    impl<A> Provide<ControlPort> for TaskSink<A>
    where
        A: 'static + ArconType,
    {
        fn handle(&mut self, _event: ControlEvent) -> () {}
    }

    impl<A> Actor for TaskSink<A>
    where
        A: 'static + ArconType,
    {
        fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
            if let Some(msg) = msg.downcast_ref::<ArconElement<A>>() {
                self.result.push(msg.data);
            }
        }
        fn receive_message(&mut self, _sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {}
    }

    impl<A> Provide<ChannelPort<A>> for TaskSink<A>
    where
        A: 'static + ArconType,
    {
        fn handle(&mut self, msg: ArconElement<A>) -> () {
            self.result.push(msg.data);
        }
    }
    impl<A> Require<ChannelPort<A>> for TaskSink<A>
    where
        A: 'static + ArconType,
    {
        fn handle(&mut self, _event: ()) -> () {}
    }
}
