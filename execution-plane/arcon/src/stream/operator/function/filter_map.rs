// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconElement, ArconEvent, ArconNever, ArconType, Epoch, Watermark},
    stream::operator::{Operator, OperatorContext},
    util::SafelySendableFn,
};
use arcon_error::ArconResult;

/// An Arcon operator for filter-mapping
///
/// IN: Input Event
/// OUT: Output Event
pub struct FilterMap<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    udf: &'static dyn SafelySendableFn(IN) -> Option<OUT>,
}

impl<IN, OUT> FilterMap<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    pub fn new(udf: &'static dyn SafelySendableFn(IN) -> Option<OUT>) -> Self {
        FilterMap { udf }
    }

    #[inline]
    pub fn run_udf(&self, event: IN) -> Option<OUT> {
        (self.udf)(event)
    }
}

impl<IN, OUT> Operator for FilterMap<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    type IN = IN;
    type OUT = OUT;
    type TimerState = ArconNever;

    fn handle_element(&mut self, element: ArconElement<IN>, mut ctx: OperatorContext<Self>) {
        if let Some(data) = element.data {
            if let Some(result) = self.run_udf(data) {
                let out_elem = ArconElement {
                    data: Some(result),
                    timestamp: element.timestamp,
                };
                ctx.output(ArconEvent::Element(out_elem));
            }
        }
    }

    fn handle_watermark(&mut self, _w: Watermark, _ctx: OperatorContext<Self>) {}
    fn handle_epoch(
        &mut self,
        _epoch: Epoch,
        _ctx: OperatorContext<Self>,
    ) -> Option<ArconResult<Vec<u8>>> {
        None
    }
    fn handle_timeout(&mut self, _timeout: Self::TimerState, _ctx: OperatorContext<Self>) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{prelude::*, timer};
    use std::str::FromStr;

    #[test]
    fn filter_map_test() {
        let mut pipeline = ArconPipeline::new();
        let system = pipeline.system();

        let comp = system.create(move || DebugNode::<u32>::new());
        system.start(&comp);

        let actor_ref: ActorRefStrong<ArconMessage<u32>> =
            comp.actor_ref().hold().expect("failed to fetch");
        let channel_strategy =
            ChannelStrategy::Forward(Forward::new(Channel::Local(actor_ref), 1.into()));

        fn filter_map_fn(s: String) -> Option<u32> {
            u32::from_str(&s).ok()
        }

        let filter_map_node = system.create(move || {
            Node::new(
                String::from("filter_map_node"),
                0.into(),
                vec![1.into()],
                channel_strategy,
                FilterMap::new(&filter_map_fn),
                Box::new(InMemory::new("test".as_ref()).unwrap()),
                timer::none(),
            )
        });
        system.start(&filter_map_node);

        let input_one = ArconEvent::Element(ArconElement::new(String::from("1")));
        let input_two = ArconEvent::Element(ArconElement::new(String::from("notanu32")));
        let input_three = ArconEvent::Element(ArconElement::new(String::from("2")));

        let msg = ArconMessage {
            events: vec![
                input_one.into(),
                input_two.into(),
                input_three.into(),
                ArconEvent::Death("die".into()).into(),
            ],
            sender: NodeID::new(1),
        };
        let filter_map_ref: ActorRefStrong<ArconMessage<String>> =
            filter_map_node.actor_ref().hold().expect("failed to fetch");

        filter_map_ref.tell(msg);

        std::thread::sleep(std::time::Duration::from_secs(1));
        {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.data.len(), 2);
            assert_eq!(comp_inspect.data[0].data, Some(1));
            assert_eq!(comp_inspect.data[1].data, Some(2));
        }

        pipeline.shutdown();
    }
}
