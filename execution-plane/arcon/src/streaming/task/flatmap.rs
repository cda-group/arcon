use crate::prelude::*;
use core::marker::PhantomData;
use std::sync::Arc;
use weld::WeldContext;

/// FlatMap task
///
/// IN: Input Event
/// OUT: Output Event
pub struct FlatMap<IN, OUT>
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

impl<IN, OUT> FlatMap<IN, OUT>
where
    IN: 'static + ArconType,
    OUT: 'static + ArconType,
{
    pub fn new(udf: Arc<Module>) -> Self {
        let ctx = WeldContext::new(&udf.conf()).unwrap();
        FlatMap {
            udf: udf.clone(),
            udf_ctx: ctx,
            metric: TaskMetric::new(),
            _in: PhantomData,
            _out: PhantomData,
        }
    }

    fn run_udf(&mut self, event: &IN) -> ArconResult<ArconVec<OUT>> {
        let run: ModuleRun<ArconVec<OUT>> = self.udf.run(event, &mut self.udf_ctx)?;
        let ns = run.1;
        self.metric.update_avg(ns);
        Ok(run.0)
    }
}

impl<IN, OUT> Task<IN, OUT> for FlatMap<IN, OUT>
where
    IN: 'static + ArconType,
    OUT: 'static + ArconType,
{
    fn handle_watermark(&mut self, _w: Watermark) -> ArconResult<Vec<ArconEvent<OUT>>> {
        Ok(Vec::new())
    }

    fn handle_element(&mut self, element: ArconElement<IN>) -> ArconResult<Vec<ArconEvent<OUT>>> {
        let mut ret = Vec::new();
        let result = self.run_udf(&(element.data))?;
        for i in 0..result.len {
            ret.push(ArconEvent::Element(ArconElement {
                data: result[i as usize],
                timestamp: element.timestamp,
            }));
        }
        Ok(ret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::DebugSink;

    #[test]
    fn flatmap_unit_test() {
        let weld_code = String::from("|x: vec[i32]| map(x, |a: i32| a + i32(5))");
        let module = Arc::new(Module::new(weld_code).unwrap());
        let mut flatmap = FlatMap::<ArconVec<i32>, i32>::new(module);

        let vec: Vec<i32> = vec![1, 2, 3, 4, 5];
        let arcon_vec = ArconVec::new(vec);
        let input = ArconElement::new(arcon_vec);

        let exec_result = flatmap.handle_element(input);
        let expected: Vec<i32> = vec![6, 7, 8, 9, 10];
        let mut result_vec = Vec::new();

        if let Ok(result) = exec_result {
            for event in result {
                if let ArconEvent::Element(element) = event {
                    result_vec.push(element.data)
                }
            }
        }
        assert_eq!(result_vec, expected);
    }

    #[test]
    fn flatmap_integration_test() {
        let system = KompactConfig::default().build().expect("KompactSystem");

        let sink_comp = system.create_and_start(move || {
            let sink: DebugSink<i32> = DebugSink::new();
            sink
        });

        let actor_ref: ActorRef<ArconMessage<i32>> = sink_comp.actor_ref();
        let channel = Channel::Local(actor_ref);
        let channel_strategy: Box<ChannelStrategy<i32>> = Box::new(Forward::new(channel));

        let weld_code = String::from("|x: vec[i32]| map(x, |a: i32| a + i32(5))");
        let module = Arc::new(Module::new(weld_code).unwrap());
        let flatmap_node = system.create_and_start(move || {
            Node::<ArconVec<i32>, i32>::new(
                "node1".to_string(),
                vec!["test".to_string()],
                channel_strategy,
                Box::new(FlatMap::<ArconVec<i32>, i32>::new(module)),
            )
        });

        let vec: Vec<i32> = vec![1, 2, 3, 4, 5];
        let arcon_vec = ArconVec::new(vec);

        let target_ref = flatmap_node.actor_ref();
        target_ref.tell(ArconMessage::<ArconVec<i32>>::element(
            arcon_vec,
            Some(0),
            "test".to_string(),
        ));

        std::thread::sleep(std::time::Duration::from_secs(3));
        {
            let comp_inspect = &sink_comp.definition().lock().unwrap();
            let expected: Vec<i32> = vec![6, 7, 8, 9, 10];
            for i in 0..5 {
                assert_eq!(comp_inspect.data[i].data, expected[i]);
            }
        }
        let _ = system.shutdown();
    }
}
