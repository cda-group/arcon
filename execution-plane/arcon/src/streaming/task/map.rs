use crate::prelude::*;
use crate::weld::*;
use std::marker::PhantomData;
use std::sync::Arc;

/// Map task
///
/// IN: Input Event
/// OUT: Output Event
pub struct Map<IN, OUT>
where
    IN: 'static + ArconType,
    OUT: 'static + ArconType,
{
    udf: Arc<Module>,
    udf_ctx: WeldContext,
    metric: TaskMetric,
    _in: PhantomData<IN>,
    _out: PhantomData<OUT>,
}

impl<IN, OUT> Map<IN, OUT>
where
    IN: 'static + ArconType,
    OUT: 'static + ArconType,
{
    pub fn new(udf: Arc<Module>) -> Self {
        let ctx = WeldContext::new(&udf.conf()).unwrap();
        Map {
            udf: udf.clone(),
            udf_ctx: ctx,
            metric: TaskMetric::new(),
            _in: PhantomData,
            _out: PhantomData,
        }
    }

    fn run_udf(&mut self, event: &IN) -> ArconResult<OUT> {
        let run: ModuleRun<OUT> = self.udf.run(event, &mut self.udf_ctx)?;
        let ns = run.1;
        self.metric.update_avg(ns);
        Ok(run.0)
    }
}

impl<IN, OUT> Task<IN, OUT> for Map<IN, OUT>
where
    IN: 'static + ArconType,
    OUT: 'static + ArconType,
{
    fn handle_element(&mut self, event: ArconElement<IN>) -> ArconResult<Vec<ArconEvent<OUT>>> {
        let data = self.run_udf(&(event.data))?;
        return Ok(vec![ArconEvent::Element(ArconElement {
            data,
            timestamp: event.timestamp,
        })]);
    }

    fn handle_watermark(&mut self, _w: Watermark) -> ArconResult<Vec<ArconEvent<OUT>>> {
        Ok(Vec::new())
    }
    fn handle_epoch(&mut self, _epoch: Epoch) -> ArconResult<Vec<u8>> {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_unit_test() {
        let weld_code = String::from("|x: i32| x + 10");
        let module = Arc::new(Module::new(weld_code).unwrap());
        let mut map = Map::<i32, i32>::new(module);

        let input_one = ArconElement::new(6 as i32);
        let input_two = ArconElement::new(7 as i32);
        let r1 = map.handle_element(input_one);
        let r2 = map.handle_element(input_two);
        let mut result_vec = Vec::new();

        result_vec.push(r1);
        result_vec.push(r2);

        let expected: Vec<i32> = vec![16, 17];
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
    fn map_integration_test() {
        let system = KompactConfig::default().build().expect("KompactSystem");

        let sink_comp = system.create_and_start(move || {
            let sink: DebugSink<i32> = DebugSink::new();
            sink
        });

        let actor_ref: ActorRef<ArconMessage<i32>> = sink_comp.actor_ref();
        let channel = Channel::Local(actor_ref);
        let channel_strategy: Box<ChannelStrategy<i32>> = Box::new(Forward::new(channel));

        let weld_code = String::from("|x: i32| x + 10");
        let module = Arc::new(Module::new(weld_code).unwrap());
        let map_node = system.create_and_start(move || {
            Node::<i32, i32>::new(
                1.into(),
                vec![0.into()],
                channel_strategy,
                Box::new(Map::<i32, i32>::new(module)),
            )
        });

        let input_one = ArconMessage::element(6 as i32, None, 0.into());
        let input_two = ArconMessage::element(7 as i32, None, 0.into());
        let target_ref = map_node.actor_ref();
        target_ref.tell(input_one);
        target_ref.tell(input_two);

        std::thread::sleep(std::time::Duration::from_secs(3));
        {
            let comp_inspect = &sink_comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.data[0].data, 16);
            assert_eq!(comp_inspect.data[1].data, 17);
        }
        let _ = system.shutdown();
    }
}
