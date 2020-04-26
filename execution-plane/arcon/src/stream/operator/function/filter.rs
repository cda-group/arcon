// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconElement, ArconEvent, ArconNever, ArconType, Epoch, Watermark},
    stream::operator::{Operator, OperatorContext},
    util::SafelySendableFn,
};

/// IN: Input Event
pub struct Filter<IN>
where
    IN: 'static + ArconType,
{
    udf: &'static dyn SafelySendableFn(&IN) -> bool,
}

impl<IN> Filter<IN>
where
    IN: 'static + ArconType,
{
    pub fn new(udf: &'static dyn SafelySendableFn(&IN) -> bool) -> Self {
        Filter { udf }
    }

    #[inline]
    pub fn run_udf(&self, element: &IN) -> bool {
        (self.udf)(element)
    }
}

impl<IN> Operator for Filter<IN>
where
    IN: 'static + ArconType,
{
    type IN = IN;
    type OUT = IN;
    type TimerState = ArconNever;

    fn handle_element(&mut self, element: ArconElement<IN>, mut ctx: OperatorContext<Self>) {
        if let Some(data) = &element.data {
            if self.run_udf(&data) {
                ctx.output(ArconEvent::Element(element));
            }
        }
    }

    fn handle_watermark(&mut self, _w: Watermark, _ctx: OperatorContext<Self>) -> () {}
    fn handle_epoch(&mut self, _epoch: Epoch, _ctx: OperatorContext<Self>) {}
    fn handle_timeout(&mut self, _timeout: Self::TimerState, _ctx: OperatorContext<Self>) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;

    #[test]
    fn filter_test() {
        let mut pipeline = ArconPipeline::new();
        let pool_info = pipeline.get_pool_info();
        let system = pipeline.system();

        let comp = system.create(move || DebugNode::<i32>::new());
        system.start(&comp);

        let actor_ref: ActorRefStrong<ArconMessage<i32>> =
            comp.actor_ref().hold().expect("failed to fetch");
        let channel_strategy =
            ChannelStrategy::Forward(Forward::new(Channel::Local(actor_ref), 1.into(), pool_info));

        fn filter_fn(x: &i32) -> bool {
            x < &8
        }

        let filter_node = system.create(move || {
            Node::new(
                String::from("filter_node"),
                0.into(),
                vec![1.into()],
                channel_strategy,
                Filter::new(&filter_fn),
                Box::new(InMemory::new("test".as_ref()).unwrap()),
                timer::none,
            )
        });
        system.start(&filter_node);

        let input_one = ArconEvent::Element(ArconElement::new(6 as i32));
        let input_two = ArconEvent::Element(ArconElement::new(7 as i32));
        let input_three = ArconEvent::Element(ArconElement::new(8 as i32));
        let input_four = ArconEvent::Element(ArconElement::new(9 as i32));

        let msg = ArconMessage {
            events: vec![
                input_one.into(),
                input_two.into(),
                input_three.into(),
                input_four.into(),
                ArconEvent::Death(String::from("die")).into(),
            ]
            .into(),
            sender: NodeID::new(1),
        };

        let filter_ref: ActorRefStrong<ArconMessage<i32>> =
            filter_node.actor_ref().hold().expect("failed to fetch");

        filter_ref.tell(msg);

        std::thread::sleep(std::time::Duration::from_secs(1));
        {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.data.len(), 2);
        }
        pipeline.shutdown();
    }
}
