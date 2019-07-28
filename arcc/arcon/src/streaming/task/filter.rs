use crate::data::{ArconElement, ArconType};
use crate::error::*;
use crate::messages::protobuf::*;
use crate::streaming::channel::strategy::ChannelStrategy;
use crate::streaming::channel::{Channel, ChannelPort};
use crate::streaming::task::{get_remote_msg, TaskMetric};
use crate::weld::*;
use kompact::*;
use std::sync::Arc;
use weld::*;

#[derive(ComponentDefinition)]
pub struct Filter<A, B>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<A>> + 'static + Clone,
{
    ctx: ComponentContext<Self>,
    _in_channels: Vec<Channel<A, B, Self>>,
    out_channels: Box<ChannelStrategy<A, B, Self>>,
    pub event_port: ProvidedPort<ChannelPort<A>, Self>,
    udf: Arc<Module>,
    udf_ctx: WeldContext,
    metric: TaskMetric,
}

impl<A, B> Filter<A, B>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<A>> + 'static + Clone,
{
    pub fn new(
        udf: Arc<Module>,
        in_channels: Vec<Channel<A, B, Self>>,
        out_channels: Box<ChannelStrategy<A, B, Self>>,
    ) -> Self {
        let ctx = WeldContext::new(&udf.conf()).unwrap();
        Filter {
            ctx: ComponentContext::new(),
            event_port: ProvidedPort::new(),
            _in_channels: in_channels,
            out_channels,
            udf: udf.clone(),
            udf_ctx: ctx,
            metric: TaskMetric::new(),
        }
    }

    fn handle_event(&mut self, event: &ArconElement<A>) -> ArconResult<()> {
        if let Ok(result) = self.run_udf(&(event.data)) {
            // Check WeldBool
            // On true, then pass along the element
            if result == 1 {
                let _ = self.push_out(*event);
            }
        } else {
            // Just report the error for now...
            error!(self.ctx.log(), "Failed to execute UDF...",);
        }
        Ok(())
    }

    fn run_udf(&mut self, event: &A) -> ArconResult<WeldBool> {
        let run: ModuleRun<WeldBool> = self.udf.run(event, &mut self.udf_ctx)?;
        let ns = run.1;
        self.metric.update_avg(ns);
        Ok(run.0)
    }

    fn push_out(&mut self, event: ArconElement<A>) -> ArconResult<()> {
        let self_ptr = self as *const Filter<A, B>;
        let _ = self.out_channels.output(event, self_ptr, None)?;
        Ok(())
    }
}

impl<A, B> Provide<ControlPort> for Filter<A, B>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<A>> + 'static + Clone,
{
    fn handle(&mut self, _event: ControlEvent) -> () {}
}

impl<A, B> Actor for Filter<A, B>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<A>> + 'static + Clone,
{
    fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
        if let Some(event) = msg.downcast_ref::<ArconElement<A>>() {
            let _ = self.handle_event(event);
        }
    }
    fn receive_message(&mut self, sender: ActorPath, ser_id: u64, buf: &mut Buf) {
        if ser_id == serialisation_ids::PBUF {
            let r: Result<StreamTaskMessage, SerError> = ProtoSer::deserialise(buf);
            if let Ok(msg) = r {
                if let Ok(event) = get_remote_msg(msg) {
                    let _ = self.handle_event(&event);
                }
            } else {
                error!(self.ctx.log(), "Failed to deserialise StreamTaskMessage",);
            }
        } else {
            error!(self.ctx.log(), "Got unexpected message from {}", sender);
        }
    }
}

impl<A, B> Require<B> for Filter<A, B>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<A>> + 'static + Clone,
{
    fn handle(&mut self, _event: B::Indication) -> () {
        // ignore
    }
}

impl<A, B> Provide<ChannelPort<A>> for Filter<A, B>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<A>> + 'static + Clone,
{
    fn handle(&mut self, event: ArconElement<A>) -> () {
        let _ = self.handle_event(&event);
    }
}

unsafe impl<A, B> Send for Filter<A, B>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<A>> + 'static + Clone,
{
}

unsafe impl<A, B> Sync for Filter<A, B>
where
    A: 'static + ArconType,
    B: Port<Request = ArconElement<A>> + 'static + Clone,
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;
    use crate::streaming::task::filter::Filter;
    use crate::streaming::task::tests::*;

    #[test]
    fn filter_task_test_local() {
        let cfg = KompactConfig::new();
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let sink_comp = system.create_and_start(move || {
            let sink: TaskSink<i32> = TaskSink::new();
            sink
        });

        let channel = Channel::Local(sink_comp.actor_ref());
        let channel_strategy: Box<
            ChannelStrategy<i32, ChannelPort<i32>, Filter<i32, ChannelPort<i32>>>,
        > = Box::new(Forward::new(channel));

        let weld_code = String::from("|x: i32| x > 5");
        let module = Arc::new(Module::new(weld_code).unwrap());
        let filter_task =
            system.create_and_start(move || Filter::new(module, Vec::new(), channel_strategy));

        let input_one = ArconElement::new(6 as i32);
        let input_two = ArconElement::new(2 as i32);

        let event_port = filter_task.on_definition(|c| c.event_port.share());
        system.trigger_r(input_one, &event_port);
        system.trigger_r(input_two, &event_port);

        std::thread::sleep(std::time::Duration::from_secs(1));
        let comp_inspect = &sink_comp.definition().lock().unwrap();
        assert_eq!(comp_inspect.result[0], 6);
        assert_eq!(comp_inspect.result.len(), 1);
        let _ = system.shutdown();
    }
}
