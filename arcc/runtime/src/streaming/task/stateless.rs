use crate::data::{ArconElement, ArconType};
use crate::error::*;
use crate::prelude::{Deserialize, Serialize};
use crate::streaming::partitioner::Partitioner;
use crate::streaming::{Channel, ChannelPort};
use crate::weld::*;
use kompact::*;
use messages::protobuf::StreamTaskMessage_oneof_payload::*;
use messages::protobuf::*;
use serde::de::DeserializeOwned;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;
use weld::*;

/// Stateless Stream Task
///
/// A: Input Event
/// B: Port type for Partitioner
/// C: Output Event
pub struct StreamTask<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<C>> + 'static + Clone,
    C: 'static + ArconType,
{
    ctx: ComponentContext<Self>,
    in_channels: Vec<Channel<C, B, Self>>,
    partitioner: Box<Partitioner<C, B, Self>>,
    udf: Arc<Module>,
    udf_avg: u64,
    udf_executions: u64,
}

impl<A, B, C> ComponentDefinition for StreamTask<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<C>> + 'static + Clone,
    C: 'static + ArconType,
{
    fn setup(&mut self, self_component: Arc<Component<Self>>) -> () {
        self.ctx_mut().initialise(self_component);
    }
    fn execute(&mut self, max_events: usize, skip: usize) -> ExecuteResult {
        ExecuteResult::new(skip, skip)
    }
    fn ctx(&self) -> &ComponentContext<Self> {
        &self.ctx
    }
    fn ctx_mut(&mut self) -> &mut ComponentContext<Self> {
        &mut self.ctx
    }
    fn type_name() -> &'static str {
        "StreamTask"
    }
}

impl<A, B, C> StreamTask<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<C>> + 'static + Clone,
    C: 'static + ArconType,
{
    pub fn new(
        udf: Arc<Module>,
        in_channels: Vec<Channel<C, B, Self>>,
        partitioner: Box<Partitioner<C, B, Self>>,
    ) -> Self {
        StreamTask {
            ctx: ComponentContext::new(),
            in_channels,
            partitioner: partitioner,
            udf: udf.clone(),
            udf_avg: 0,
            udf_executions: 0,
        }
    }

    fn handle_remote_msg(&mut self, data: StreamTaskMessage) -> ArconResult<()> {
        let payload = data.payload.unwrap();

        match payload {
            element(e) => {
                let event: A = bincode::deserialize(e.get_data()).map_err(|e| {
                    arcon_err_kind!("Failed to deserialise event with err {}", e.to_string())
                })?;
                let arcon_element = ArconElement::with_timestamp(event, e.get_timestamp());
                let _ = self.handle_event(&arcon_element);
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
        }

        Ok(())
    }

    fn handle_event(&mut self, event: &ArconElement<A>) -> ArconResult<()> {
        if let Ok(result) = self.run_udf(&(event.data)) {
            let _ = self.push_out(ArconElement::new(result));
        } else {
            // Just report the error for now...
            error!(self.ctx.log(), "Failed to execute UDF...",);
        }
        Ok(())
    }

    fn run_udf(&mut self, event: &A) -> ArconResult<C> {
        // NOTE: Decide if we want to use new context for each execution, or
        //       reuse the same one over and over...
        let ref mut ctx = WeldContext::new(&self.udf.conf()).map_err(|e| {
            weld_error!(
                "Failed to build WeldContext with err {}",
                e.message().to_string_lossy().into_owned()
            )
        })?;

        let run: ModuleRun<C> = self.udf.run(event, ctx)?;
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

    fn push_out(&mut self, event: ArconElement<C>) -> ArconResult<()> {
        let self_ptr = self as *const StreamTask<A, B, C>;
        let _ = self.partitioner.output(event, self_ptr, None)?;
        Ok(())
    }
}

impl<A, B, C> Provide<ControlPort> for StreamTask<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<C>> + 'static + Clone,
    C: 'static + ArconType,
{
    fn handle(&mut self, event: ControlEvent) -> () {}
}

impl<A, B, C> Actor for StreamTask<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<C>> + 'static + Clone,
    C: 'static + ArconType,
{
    fn receive_local(&mut self, sender: ActorRef, msg: &Any) {
        if let Some(event) = msg.downcast_ref::<ArconElement<A>>() {
            let _ = self.handle_event(event);
        }
    }
    fn receive_message(&mut self, sender: ActorPath, ser_id: u64, buf: &mut Buf) {
        if ser_id == serialisation_ids::PBUF {
            let r: Result<StreamTaskMessage, SerError> = ProtoSer::deserialise(buf);
            if let Ok(msg) = r {
                let _ = self.handle_remote_msg(msg);
            } else {
                error!(self.ctx.log(), "Failed to deserialise StreamTaskMessage",);
            }
        } else {
            error!(self.ctx.log(), "Got unexpected message from {}", sender);
        }
    }
}

impl<A, B, C> Require<B> for StreamTask<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<C>> + 'static + Clone,
    C: 'static + ArconType,
{
    fn handle(&mut self, event: B::Indication) -> () {
        // ignore
    }
}

impl<A, B, C> Provide<ChannelPort<A>> for StreamTask<A, B, C>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<C>> + 'static + Clone,
    C: 'static + ArconType,
{
    fn handle(&mut self, event: ArconElement<A>) -> () {
        let _ = self.handle_event(&event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::partitioner::forward::Forward;
    use crate::streaming::RequirePortRef;
    use kompact::default_components::*;
    use kompact::*;
    use rand::Rng;
    use std::cell::RefCell;
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
        fn handle(&mut self, event: ControlEvent) -> () {}
    }

    impl Actor for SinkActor {
        fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
            if let Some(input) = msg.downcast_ref::<ArconElement<TaskOutput>>() {
                self.result = Some((input.data).clone());
            }
        }
        fn receive_message(&mut self, sender: ActorPath, ser_id: u64, buf: &mut Buf) {
            if ser_id == serialisation_ids::PBUF {
                let r: Result<StreamTaskMessage, SerError> = ProtoSer::deserialise(buf);
                let payload = r.unwrap().payload.unwrap();

                match payload {
                    element(e) => {
                        let event: TaskOutput = bincode::deserialize(e.get_data())
                            .map_err(|e| {
                                arcon_err_kind!(
                                    "Failed to deserialise event with err {}",
                                    e.to_string()
                                )
                            })
                            .unwrap();
                        self.result = Some(event);
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
                }
            } else {
                error!(self.ctx.log(), "Got unexpected message from {}", sender);
            }
        }
    }
    impl Provide<ChannelPort<TaskOutput>> for SinkActor {
        fn handle(&mut self, event: ArconElement<TaskOutput>) -> () {
            self.result = Some(event.data);
        }
    }
    impl Require<ChannelPort<TaskOutput>> for SinkActor {
        fn handle(&mut self, event: ()) -> () {}
    }

    #[test]
    fn stream_task_local_test() {
        let mut cfg = KompactConfig::new();
        cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let sink_comp = system.create_and_start(move || SinkActor::new());

        let channel = Channel::Local(sink_comp.actor_ref());
        let mut partitioner: Box<
            Partitioner<
                TaskOutput,
                ChannelPort<TaskOutput>,
                StreamTask<TaskInput, ChannelPort<TaskOutput>, TaskOutput>,
            >,
        > = Box::new(Forward::new(channel));

        let weld_code = String::from("|id: u32, price: u64| {id, price + u64(5)}");
        let module = Arc::new(Module::new("id".to_string(), weld_code, 0, None).unwrap());
        let stream_task =
            system.create_and_start(move || StreamTask::new(module, Vec::new(), partitioner));

        let task_input = ArconElement::new(TaskInput { id: 10, price: 20 });

        stream_task
            .actor_ref()
            .tell(Box::new(task_input), &stream_task);

        std::thread::sleep(std::time::Duration::from_secs(1));
        let mut comp_inspect = &sink_comp.definition().lock().unwrap();
        assert_eq!(comp_inspect.result.unwrap().price, 25);
        system.shutdown();
    }

    #[test]
    fn stream_task_port_test() {
        let mut cfg = KompactConfig::new();
        cfg.system_components(DeadletterBox::new, NetworkConfig::default().build());
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let sink_comp = system.create_and_start(move || SinkActor::new());
        let target_port = sink_comp.on_definition(|c| c.in_port.share());

        let mut req_port: RequiredPort<
            ChannelPort<TaskOutput>,
            StreamTask<TaskInput, ChannelPort<TaskOutput>, TaskOutput>,
        > = RequiredPort::new();
        let _ = req_port.connect(target_port);

        let ref_port = RequirePortRef(Rc::new(RefCell::new(req_port)));
        let channel = Channel::Port(ref_port);
        let mut partitioner = Box::new(Forward::new(channel));

        let weld_code = String::from("|id: u32, price: u64| {id, price + u64(5)}");
        let module = Arc::new(Module::new("id".to_string(), weld_code, 0, None).unwrap());
        let stream_task =
            system.create_and_start(move || StreamTask::new(module, Vec::new(), partitioner));

        let task_input = ArconElement::new(TaskInput { id: 10, price: 20 });

        stream_task
            .actor_ref()
            .tell(Box::new(task_input), &stream_task);

        std::thread::sleep(std::time::Duration::from_secs(1));
        let mut comp_inspect = &sink_comp.definition().lock().unwrap();
        assert_eq!(comp_inspect.result.unwrap().price, 25);
        system.shutdown();
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

        let mut partitioner: Box<
            Partitioner<
                TaskOutput,
                ChannelPort<TaskOutput>,
                StreamTask<TaskInput, ChannelPort<TaskOutput>, TaskOutput>,
            >,
        > = Box::new(Forward::new(remote_channel));
        let weld_code = String::from("|id: u32, price: u64| {id, price + u64(5)}");
        let module = Arc::new(Module::new("id".to_string(), weld_code, 0, None).unwrap());
        let stream_task =
            system.create_and_start(move || StreamTask::new(module, Vec::new(), partitioner));

        let task_input = ArconElement::new(TaskInput { id: 10, price: 20 });

        stream_task
            .actor_ref()
            .tell(Box::new(task_input), &stream_task);

        std::thread::sleep(std::time::Duration::from_secs(1));
        let mut comp_inspect = &sink_comp.definition().lock().unwrap();
        assert_eq!(comp_inspect.result.unwrap().price, 25);
    }
}
