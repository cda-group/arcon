use crate::data::{ArconElement, ArconEvent, ArconType, Watermark};
use crate::error::*;
use crate::streaming::channel::strategy::ChannelStrategy;
use crate::streaming::channel::{Channel, ChannelPort};
use crate::weld::*;
use arcon_macros::arcon_task;
use kompact::*;
use std::sync::Arc;
use weld::*;

/// Stateless Stream Task
///
/// IN: Input Event
/// PORT: Port type for ChannelStrategy
/// C: Output Event
#[arcon_task]
#[derive(ComponentDefinition)]
pub struct StreamTask<IN, PORT, C>
where
    IN: 'static + ArconType,
    PORT: Port<Request = ArconEvent<C>> + 'static + Clone,
    C: 'static + ArconType,
{
    ctx: ComponentContext<Self>,
    _in_channels: Vec<Channel<C, PORT, Self>>,
    out_channels: Box<ChannelStrategy<C, PORT, Self>>,
    pub event_port: ProvidedPort<ChannelPort<IN>, Self>,
    udf: Arc<Module>,
    udf_ctx: WeldContext,
    udf_avg: u64,
    udf_executions: u64,
}

impl<IN, PORT, C> StreamTask<IN, PORT, C>
where
    IN: 'static + ArconType,
    PORT: Port<Request = ArconEvent<C>> + 'static + Clone,
    C: 'static + ArconType,
{
    pub fn new(
        udf: Arc<Module>,
        in_channels: Vec<Channel<C, PORT, Self>>,
        out_channels: Box<ChannelStrategy<C, PORT, Self>>,
    ) -> Self {
        let ctx = WeldContext::new(&udf.conf()).unwrap();
        StreamTask {
            ctx: ComponentContext::new(),
            event_port: ProvidedPort::new(),
            _in_channels: in_channels,
            out_channels,
            udf: udf.clone(),
            udf_ctx: ctx,
            udf_avg: 0,
            udf_executions: 0,
        }
    }

    fn handle_element(&mut self, e: &ArconElement<IN>) -> ArconResult<()> {
        if let Ok(result) = self.run_udf(&(e.data)) {
            let _ = self.push_out(ArconEvent::Element(ArconElement::new(result)))?;
        } else {
            // Just report the error for now...
            error!(self.ctx.log(), "Failed to execute UDF...",);
        }
        Ok(())
    }

    fn handle_watermark(&mut self, _w: Watermark) -> ArconResult<()> {
        unimplemented!();
    }

    fn run_udf(&mut self, event: &IN) -> ArconResult<C> {
        let run: ModuleRun<C> = self.udf.run(event, &mut self.udf_ctx)?;
        let ns = run.1;
        self.update_avg(ns);
        Ok(run.0)
    }
    fn update_avg(&mut self, ns: u64) {
        if self.udf_executions == 0 {
            self.udf_avg = ns;
        } else {
            let ema: i32 = (ns as i32 - self.udf_avg as i32)
                * (2 / (self.udf_executions + 1)) as i32
                + self.udf_avg as i32;
            self.udf_avg = ema as u64;
        }
        self.udf_executions += 1;
    }

    fn push_out(&mut self, event: ArconEvent<C>) -> ArconResult<()> {
        let self_ptr = self as *const StreamTask<IN, PORT, C>;
        let _ = self.out_channels.output(event, self_ptr)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::channel::strategy::forward::Forward;
    use crate::streaming::channel::RequirePortRef;
    use kompact::default_components::*;
    use std::cell::UnsafeCell;
    use std::rc::Rc;
    use std::sync::Arc;

    #[arcon]
    #[derive(Hash)]
    pub struct TaskInput {
        id: u32,
        price: u64,
    }

    // Just to make it clear...
    #[arcon]
    #[derive(Hash)]
    pub struct TaskOutput {
        id: u32,
        price: u64,
    }

    #[derive(ComponentDefinition)]
    #[allow(dead_code)]
    pub struct SinkActor {
        ctx: ComponentContext<SinkActor>,
        in_port: ProvidedPort<ChannelPort<TaskOutput>, SinkActor>,
        pub result: Option<TaskOutput>,
    }

    impl SinkActor {
        pub fn new() -> SinkActor {
            SinkActor {
                ctx: ComponentContext::new(),
                in_port: ProvidedPort::new(),
                result: None,
            }
        }
    }
    impl Provide<ControlPort> for SinkActor {
        fn handle(&mut self, _event: ControlEvent) -> () {}
    }

    impl Actor for SinkActor {
        fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
            if let Some(event) = msg.downcast_ref::<ArconEvent<TaskOutput>>() {
                match event {
                    ArconEvent::Element(e) => {
                        self.result = Some(e.data);
                    }
                    _ => {}
                }
            }
        }
        fn receive_message(&mut self, sender: ActorPath, ser_id: u64, buf: &mut Buf) {
            if ser_id == serialisation_ids::PBUF {
                let r: Result<StreamTaskMessage, SerError> = ProtoSer::deserialise(buf);
                if let Ok(msg) = r {
                    if let Ok(event) = ArconEvent::from_remote(msg) {
                        match event {
                            ArconEvent::Element(e) => {
                                self.result = Some(e.data);
                            }
                            _ => {}
                        }
                    } else {
                        error!(self.ctx.log(), "Failed to convert remote message");
                    }
                } else {
                    error!(self.ctx.log(), "Failed to deserialise StreamTaskMessage",);
                }
            } else {
                error!(self.ctx.log(), "Got unexpected message from {}", sender);
            }
        }
    }
    impl Provide<ChannelPort<TaskOutput>> for SinkActor {
        fn handle(&mut self, event: ArconEvent<TaskOutput>) -> () {
            match event {
                ArconEvent::Element(e) => {
                    self.result = Some(e.data);
                }
                _ => {}
            }
        }
    }
    impl Require<ChannelPort<TaskOutput>> for SinkActor {
        fn handle(&mut self, _event: ()) -> () {}
    }

    #[test]
    fn stream_task_local_test() {
        let cfg = KompactConfig::new();
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let sink_comp = system.create_and_start(move || SinkActor::new());

        let channel = Channel::Local(sink_comp.actor_ref());
        let channel_strategy: Box<
            ChannelStrategy<
                TaskOutput,
                ChannelPort<TaskOutput>,
                StreamTask<TaskInput, ChannelPort<TaskOutput>, TaskOutput>,
            >,
        > = Box::new(Forward::new(channel));

        let weld_code = String::from("|id: u32, price: u64| {id, price + u64(5)}");
        let module = Arc::new(Module::new(weld_code).unwrap());
        let stream_task =
            system.create_and_start(move || StreamTask::new(module, Vec::new(), channel_strategy));

        let task_input = ArconElement::new(TaskInput { id: 10, price: 20 });

        let event_port = stream_task.on_definition(|c| c.event_port.share());
        system.trigger_r(ArconEvent::Element(task_input), &event_port);
        std::thread::sleep(std::time::Duration::from_secs(1));
        let comp_inspect = &sink_comp.definition().lock().unwrap();
        assert_eq!(comp_inspect.result.unwrap().price, 25);
        let _ = system.shutdown();
    }

    #[test]
    fn stream_task_port_test() {
        let cfg = KompactConfig::new();
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let sink_comp = system.create_and_start(move || SinkActor::new());
        let target_port = sink_comp.on_definition(|c| c.in_port.share());

        let mut req_port: RequiredPort<
            ChannelPort<TaskOutput>,
            StreamTask<TaskInput, ChannelPort<TaskOutput>, TaskOutput>,
        > = RequiredPort::new();
        let _ = req_port.connect(target_port);

        let ref_port = RequirePortRef(Rc::new(UnsafeCell::new(req_port)));
        let channel = Channel::Port(ref_port);
        let channel_strategy = Box::new(Forward::new(channel));

        let weld_code = String::from("|id: u32, price: u64| {id, price + u64(5)}");
        let module = Arc::new(Module::new(weld_code).unwrap());
        let stream_task =
            system.create_and_start(move || StreamTask::new(module, Vec::new(), channel_strategy));

        let task_input = ArconElement::new(TaskInput { id: 10, price: 20 });

        stream_task
            .actor_ref()
            .tell(Box::new(ArconEvent::Element(task_input)), &stream_task);

        std::thread::sleep(std::time::Duration::from_secs(1));
        let comp_inspect = &sink_comp.definition().lock().unwrap();
        assert_eq!(comp_inspect.result.unwrap().price, 25);
        let _ = system.shutdown();
    }

    #[test]
    fn stream_task_remote_test() {
        let (system, remote) = {
            let system = || {
                let mut cfg = KompactConfig::new();
                cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
                KompactSystem::new(cfg).expect("KompactSystem")
            };
            (system(), system())
        };

        let sink_comp = remote.create_and_start(move || SinkActor::new());
        let _ = remote.register_by_alias(&sink_comp, "sink_comp");

        let remote_path = ActorPath::Named(NamedPath::with_system(
            remote.system_path(),
            vec!["sink_comp".into()],
        ));

        let remote_channel = Channel::Remote(remote_path);

        let channel_strategy: Box<
            ChannelStrategy<
                TaskOutput,
                ChannelPort<TaskOutput>,
                StreamTask<TaskInput, ChannelPort<TaskOutput>, TaskOutput>,
            >,
        > = Box::new(Forward::new(remote_channel));
        let weld_code = String::from("|id: u32, price: u64| {id, price + u64(5)}");
        let module = Arc::new(Module::new(weld_code).unwrap());
        let stream_task =
            system.create_and_start(move || StreamTask::new(module, Vec::new(), channel_strategy));

        let task_input = ArconElement::new(TaskInput { id: 10, price: 20 });

        stream_task
            .actor_ref()
            .tell(Box::new(ArconEvent::Element(task_input)), &stream_task);

        std::thread::sleep(std::time::Duration::from_secs(1));
        let comp_inspect = &sink_comp.definition().lock().unwrap();
        assert_eq!(comp_inspect.result.unwrap().price, 25);
    }
}
