pub mod filter;
pub mod flatmap;
pub mod manager;
pub mod map;

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

#[cfg(test)]
mod tests {
    use crate::data::{ArconEvent, ArconType, Watermark};
    use kompact::*;
    /// A component that is used during testing of Tasks..
    #[derive(ComponentDefinition)]
    #[allow(dead_code)]
    pub struct TaskSink<A>
    where
        A: 'static + ArconType,
    {
        ctx: ComponentContext<Self>,
        pub result: Vec<A>,
        pub watermarks: Vec<Watermark>,
    }

    impl<A> TaskSink<A>
    where
        A: 'static + ArconType,
    {
        pub fn new() -> Self {
            TaskSink {
                ctx: ComponentContext::new(),
                result: Vec::new(),
                watermarks: Vec::new(),
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
            if let Some(event) = msg.downcast_ref::<ArconEvent<A>>() {
                match event {
                    ArconEvent::Element(e) => {
                        self.result.push(e.data);
                    }
                    ArconEvent::Watermark(w) => {
                        self.watermarks.push(*w);
                    }
                }
            }
        }
        fn receive_message(&mut self, _sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {}
    }
}
