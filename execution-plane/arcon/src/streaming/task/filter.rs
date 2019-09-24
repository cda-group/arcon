use crate::prelude::*;
use std::marker::PhantomData;
use std::sync::Arc;
use weld::WeldContext;

pub struct Filter<IN>
where
    IN: 'static + ArconType,
{
    udf: Arc<Module>,
    udf_ctx: WeldContext,
    metric: TaskMetric,
    _in: PhantomData<IN>,
}

impl<IN> Filter<IN>
where
    IN: 'static + ArconType,
{
    pub fn new(udf: Arc<Module>) -> Self {
        let ctx = WeldContext::new(&udf.conf()).unwrap();
        Filter {
            udf: udf.clone(),
            udf_ctx: ctx,
            metric: TaskMetric::new(),
            _in: PhantomData,
        }
    }

    fn run_udf(&mut self, event: &IN) -> ArconResult<WeldBool> {
        let run: ModuleRun<WeldBool> = self.udf.run(event, &mut self.udf_ctx)?;
        let ns = run.1;
        self.metric.update_avg(ns);
        Ok(run.0)
    }
}

impl<IN> Task<IN, IN> for Filter<IN>
where
    IN: 'static + ArconType,
{
    fn handle_watermark(&mut self, _w: Watermark) -> ArconResult<Vec<ArconEvent<IN>>> {
        Ok(Vec::new())
    }

    fn handle_element(&mut self, element: ArconElement<IN>) -> ArconResult<Vec<ArconEvent<IN>>> {
        let result = self.run_udf(&(element.data))?;
        if result == 1 {
            return Ok(vec![ArconEvent::Element(element)]);
        } else {
            Ok(Vec::new())
        }
    }
    fn handle_epoch(&mut self, _epoch: Epoch) -> ArconResult<Vec<u8>> {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_unit_test() {
        let weld_code = String::from("|x: i32| x > 5");
        let module = Arc::new(Module::new(weld_code).unwrap());
        let mut filter = Filter::<i32>::new(module);

        let i1 = ArconElement::new(6 as i32);
        let i2 = ArconElement::new(2 as i32);

        let mut result_vec = Vec::new();
        let r1 = filter.handle_element(i1);
        let r2 = filter.handle_element(i2);
        result_vec.push(r1);
        result_vec.push(r2);

        let expected: Vec<i32> = vec![6];
        let mut results = Vec::new();
        for r in result_vec {
            if let Ok(result) = r {
                for event in result {
                    if let ArconEvent::Element(element) = event {
                        results.push(element.data)
                    }
                }
            }
        }
        assert_eq!(results, expected);
    }

    #[test]
    fn filter_integration_test() {
        let cfg = KompactConfig::new();
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let sink_comp = system.create_and_start(move || {
            let sink: DebugSink<i32> = DebugSink::new();
            sink
        });

        let channel = Channel::Local(sink_comp.actor_ref());
        let channel_strategy: Box<ChannelStrategy<i32>> = Box::new(Forward::new(channel));

        let weld_code = String::from("|x: i32| x > 5");
        let module = Arc::new(Module::new(weld_code).unwrap());
        let filter_task = system.create_and_start(move || {
            Node::<i32, i32>::new(
                1.into(),
                vec![0.into()],
                channel_strategy,
                Box::new(Filter::<i32>::new(module)),
            )
        });

        let input_one = ArconMessage::element(6 as i32, None, 0.into());
        let input_two = ArconMessage::element(2 as i32, None, 0.into());

        let target_ref = filter_task.actor_ref();
        target_ref.tell(Box::new(input_one), &target_ref);
        target_ref.tell(Box::new(input_two), &target_ref);

        std::thread::sleep(std::time::Duration::from_secs(1));
        let comp_inspect = &sink_comp.definition().lock().unwrap();
        assert_eq!(comp_inspect.data[0].data, 6);
        assert_eq!(comp_inspect.data.len(), 1);
        let _ = system.shutdown();
    }
}
