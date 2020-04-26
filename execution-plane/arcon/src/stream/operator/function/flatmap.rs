// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconElement, ArconEvent, ArconNever, ArconType, Epoch, Watermark},
    stream::operator::{Operator, OperatorContext},
    util::SafelySendableFn,
};

/// IN: Input Event
/// OUT: Output Event
pub struct FlatMap<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    udf: &'static dyn SafelySendableFn(&IN) -> Vec<OUT>,
}

impl<IN, OUT> FlatMap<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    pub fn new(udf: &'static dyn SafelySendableFn(&IN) -> Vec<OUT>) -> Self {
        FlatMap { udf }
    }

    #[inline]
    pub fn run_udf(&self, event: &IN) -> Vec<OUT> {
        (self.udf)(event)
    }
}

impl<IN, OUT> Operator for FlatMap<IN, OUT>
where
    IN: 'static + ArconType,
    OUT: 'static + ArconType,
{
    type IN = IN;
    type OUT = OUT;
    type TimerState = ArconNever;

    fn handle_element(&mut self, element: ArconElement<IN>, mut ctx: OperatorContext<Self>) {
        if let Some(data) = element.data {
            let result = self.run_udf(&(data));
            for item in result {
                ctx.output(ArconEvent::Element(ArconElement {
                    data: Some(item),
                    timestamp: element.timestamp,
                }));
            }
        }
    }

    fn handle_watermark(&mut self, _w: Watermark, _ctx: OperatorContext<Self>) {}
    fn handle_epoch(&mut self, _epoch: Epoch, _ctx: OperatorContext<Self>) {}
    fn handle_timeout(&mut self, _timeout: Self::TimerState, _ctx: OperatorContext<Self>) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{prelude::*, timer};

    #[test]
    fn flatmap_test() {
        let mut pipeline = ArconPipeline::new();
        let pool_info = pipeline.get_pool_info();
        let system = pipeline.system();

        let comp = system.create(move || DebugNode::<i32>::new());
        system.start(&comp);

        let actor_ref: ActorRefStrong<ArconMessage<i32>> =
            comp.actor_ref().hold().expect("failed to fetch");
        let channel_strategy =
            ChannelStrategy::Forward(Forward::new(Channel::Local(actor_ref), 1.into(), pool_info));

        fn flatmap_fn(x: &i32) -> Vec<i32> {
            (0..*x).map(|x| x + 5).collect()
        }
        let flatmap_node = system.create(move || {
            Node::new(
                String::from("flatmap_node"),
                0.into(),
                vec![1.into()],
                channel_strategy,
                FlatMap::new(&flatmap_fn),
                Box::new(InMemory::new("test".as_ref()).unwrap()),
                timer::none,
            )
        });
        system.start(&flatmap_node);

        let input_one = ArconEvent::Element(ArconElement::new(6 as i32));
        let msg = ArconMessage {
            events: vec![
                input_one.into(),
                ArconEvent::Death(String::from("die")).into(),
            ]
            .into(),
            sender: NodeID::new(1),
        };
        let flatmap_ref: ActorRefStrong<ArconMessage<i32>> =
            flatmap_node.actor_ref().hold().expect("failed to fetch");

        flatmap_ref.tell(msg);

        std::thread::sleep(std::time::Duration::from_secs(1));
        {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.data.len(), 6);
        }
        pipeline.shutdown();
    }
}
