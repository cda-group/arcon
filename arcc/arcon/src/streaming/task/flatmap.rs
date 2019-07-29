use crate::data::{ArconElement, ArconEvent, ArconType, ArconVec, Watermark};
use crate::error::*;
use crate::streaming::channel::strategy::ChannelStrategy;
use crate::streaming::channel::{Channel, ChannelPort};
use crate::streaming::task::TaskMetric;
use crate::weld::*;
use arcon_macros::arcon_task;
use kompact::*;
use std::sync::Arc;
use weld::*;

/// FlatMap task
///
/// IN: Input Event
/// PORT: Port type for ChannelStrategy
/// C: Output Event
#[arcon_task]
#[derive(ComponentDefinition)]
pub struct FlatMap<IN, PORT, C>
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
    metric: TaskMetric,
}

impl<IN, PORT, C> FlatMap<IN, PORT, C>
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
        FlatMap {
            ctx: ComponentContext::new(),
            event_port: ProvidedPort::new(),
            _in_channels: in_channels,
            out_channels,
            udf: udf.clone(),
            udf_ctx: ctx,
            metric: TaskMetric::new(),
        }
    }

    fn handle_watermark(&mut self, _w: Watermark) -> ArconResult<()> {
        unimplemented!();
    }

    fn handle_element(&mut self, event: &ArconElement<IN>) -> ArconResult<()> {
        if let Ok(result) = self.run_udf(&(event.data)) {
            // Result should be an ArconVec of elements
            // iterate over it and send
            for i in 0..result.len {
                let _ = self.push_out(ArconEvent::Element(ArconElement::new(result[i as usize])));
            }
        } else {
            // Just report the error for now...
            error!(self.ctx.log(), "Failed to execute UDF...",);
        }
        Ok(())
    }

    fn run_udf(&mut self, event: &IN) -> ArconResult<ArconVec<C>> {
        let run: ModuleRun<ArconVec<C>> = self.udf.run(event, &mut self.udf_ctx)?;
        let ns = run.1;
        self.metric.update_avg(ns);
        Ok(run.0)
    }

    fn push_out(&mut self, event: ArconEvent<C>) -> ArconResult<()> {
        let self_ptr = self as *const FlatMap<IN, PORT, C>;
        let _ = self.out_channels.output(event, self_ptr)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;
    use crate::streaming::task::tests::*;

    #[test]
    fn flatmap_task_test_local() {
        let cfg = KompactConfig::new();
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let sink_comp = system.create_and_start(move || {
            let sink: TaskSink<i32> = TaskSink::new();
            sink
        });

        let channel = Channel::Local(sink_comp.actor_ref());
        let channel_strategy: Box<
            ChannelStrategy<i32, ChannelPort<i32>, FlatMap<ArconVec<i32>, ChannelPort<i32>, i32>>,
        > = Box::new(Forward::new(channel));

        let weld_code = String::from("|x: vec[i32]| map(x, |a: i32| a + i32(5))");
        let module = Arc::new(Module::new(weld_code).unwrap());
        let filter_task =
            system.create_and_start(move || FlatMap::new(module, Vec::new(), channel_strategy));

        let vec: Vec<i32> = vec![1, 2, 3, 4, 5];
        let arcon_vec = ArconVec::new(vec);
        let input = ArconElement::new(arcon_vec);

        let event_port = filter_task.on_definition(|c| c.event_port.share());
        system.trigger_r(ArconEvent::Element(input), &event_port);

        std::thread::sleep(std::time::Duration::from_secs(1));
        let comp_inspect = &sink_comp.definition().lock().unwrap();
        let expected: Vec<i32> = vec![6, 7, 8, 9, 10];
        assert_eq!(&comp_inspect.result, &expected);
        let _ = system.shutdown();
    }
}
