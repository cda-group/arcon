use crate::data::{ArconElement, ArconEvent, ArconType, Watermark};
use crate::error::*;
use crate::streaming::channel::strategy::ChannelStrategy;
use crate::streaming::channel::Channel;
use crate::streaming::task::TaskMetric;
use crate::weld::*;
use arcon_macros::arcon_task;
use kompact::*;
use std::sync::Arc;
use weld::*;

#[arcon_task]
#[derive(ComponentDefinition)]
pub struct Filter<IN>
where
    IN: 'static + ArconType,
{
    ctx: ComponentContext<Self>,
    _in_channels: Vec<Channel>,
    out_channels: Box<ChannelStrategy<IN>>,
    udf: Arc<Module>,
    udf_ctx: WeldContext,
    metric: TaskMetric,
}

impl<IN> Filter<IN>
where
    IN: 'static + ArconType,
{
    pub fn new(
        udf: Arc<Module>,
        in_channels: Vec<Channel>,
        out_channels: Box<ChannelStrategy<IN>>,
    ) -> Self {
        let ctx = WeldContext::new(&udf.conf()).unwrap();
        Filter {
            ctx: ComponentContext::new(),
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

    fn handle_element(&mut self, element: &ArconElement<IN>) -> ArconResult<()> {
        if let Ok(result) = self.run_udf(&(element.data)) {
            // Check WeldBool
            // On true, then pass along the element
            if result == 1 {
                let _ = self.push_out(ArconEvent::Element(*element));
            }
        } else {
            // Just report the error for now...
            error!(self.ctx.log(), "Failed to execute UDF...",);
        }
        Ok(())
    }

    fn run_udf(&mut self, event: &IN) -> ArconResult<WeldBool> {
        let run: ModuleRun<WeldBool> = self.udf.run(event, &mut self.udf_ctx)?;
        let ns = run.1;
        self.metric.update_avg(ns);
        Ok(run.0)
    }

    fn push_out(&mut self, event: ArconEvent<IN>) -> ArconResult<()> {
        let _ = self.out_channels.output(event, &self.ctx.system())?;
        Ok(())
    }
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
        let channel_strategy: Box<ChannelStrategy<i32>> =
            Box::new(Forward::new(channel));

        let weld_code = String::from("|x: i32| x > 5");
        let module = Arc::new(Module::new(weld_code).unwrap());
        let filter_task =
            system.create_and_start(move || Filter::new(module, Vec::new(), channel_strategy));

        let input_one = ArconElement::new(6 as i32);
        let input_two = ArconElement::new(2 as i32);

        let target_ref = filter_task.actor_ref();
        target_ref.tell(Box::new(ArconEvent::Element(input_one)), &target_ref);
        target_ref.tell(Box::new(ArconEvent::Element(input_two)), &target_ref);

        std::thread::sleep(std::time::Duration::from_secs(1));
        let comp_inspect = &sink_comp.definition().lock().unwrap();
        assert_eq!(comp_inspect.result[0], 6);
        assert_eq!(comp_inspect.result.len(), 1);
        let _ = system.shutdown();
    }
}
