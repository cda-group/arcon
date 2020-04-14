// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconElement, ArconEvent, ArconType, Epoch, Watermark},
    stream::operator::{Operator, OperatorContext},
    util::SafelySendableFn,
};
use arcon_error::ArconResult;

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

impl<IN> Operator<IN, IN> for Filter<IN>
where
    IN: 'static + ArconType,
{
    fn handle_element(&mut self, element: ArconElement<IN>, mut ctx: OperatorContext<IN>) {
        if let Some(data) = &element.data {
            if self.run_udf(&data) {
                ctx.output(ArconEvent::Element(element));
            }
        }
    }

    fn handle_watermark(
        &mut self,
        _w: Watermark,
        _ctx: OperatorContext<IN>,
    ) -> Option<Vec<ArconEvent<IN>>> {
        None
    }
    fn handle_epoch(
        &mut self,
        _epoch: Epoch,
        _ctx: OperatorContext<IN>,
    ) -> Option<ArconResult<Vec<u8>>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;

    #[test]
    fn filter_test() {
        let system = KompactConfig::default().build().expect("KompactSystem");
        let comp = system.create(move || DebugNode::<i32>::new());
        system.start(&comp);

        let actor_ref: ActorRefStrong<ArconMessage<i32>> =
            comp.actor_ref().hold().expect("failed to fetch");
        let channel_strategy =
            ChannelStrategy::Forward(Forward::new(Channel::Local(actor_ref), 1.into()));

        fn filter_fn(x: &i32) -> bool {
            x < &8
        }

        let filter_node = system.create(move || {
            Node::<i32, i32>::new(
                String::from("filter_node"),
                0.into(),
                vec![1.into()],
                channel_strategy,
                Box::new(Filter::new(&filter_fn)),
                Box::new(InMemory::new("test").unwrap()),
                ".".into(),
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
                ArconEvent::Death("die".into()).into(),
            ],
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
        let _ = system.shutdown();
    }
}
