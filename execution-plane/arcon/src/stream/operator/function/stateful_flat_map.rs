// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconElement, ArconEvent, ArconNever, ArconType, Epoch, Watermark},
    stream::operator::{Operator, OperatorContext},
    util::SafelySendableFn,
};

/// IN: Input Event
/// C: Returned collection type (Vec<OUT>, Option<OUT>, ...)
/// OUT: Output Event
pub struct StatefulFlatMap<IN, C, OUT>
where
    IN: ArconType,
    OUT: ArconType,
    C: IntoIterator<Item = OUT> + 'static,
{
    udf: &'static dyn SafelySendableFn(OperatorContext<Self>, IN) -> C,
}

impl<IN, C, OUT> StatefulFlatMap<IN, C, OUT>
where
    IN: ArconType,
    OUT: ArconType,
    C: IntoIterator<Item = OUT> + 'static,
{
    pub fn new(udf: &'static dyn SafelySendableFn(OperatorContext<Self>, IN) -> C) -> Self {
        StatefulFlatMap { udf }
    }
}

impl<IN, C, OUT> Operator for StatefulFlatMap<IN, C, OUT>
where
    IN: ArconType,
    OUT: ArconType,
    C: IntoIterator<Item = OUT> + 'static,
{
    type IN = IN;
    type OUT = OUT;
    type TimerState = ArconNever;

    fn handle_element(&mut self, element: ArconElement<IN>, mut ctx: OperatorContext<Self>) {
        let result = (self.udf)(
            // TODO: annoying manual copy required to satisfy borrowchk
            OperatorContext::new(ctx.channel_strategy, ctx.state_backend, ctx.timer_backend),
            element.data,
        );
        for item in result {
            ctx.output(ArconEvent::Element(ArconElement {
                data: item,
                timestamp: element.timestamp,
            }));
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
    fn stateful_flatmap_test() {
        let mut pipeline = ArconPipeline::new();
        let pool_info = pipeline.get_pool_info();
        let system = pipeline.system();

        let comp = system.create(move || DebugNode::<i32>::new());
        system.start(&comp);

        let actor_ref: ActorRefStrong<ArconMessage<i32>> =
            comp.actor_ref().hold().expect("failed to fetch");
        let channel_strategy =
            ChannelStrategy::Forward(Forward::new(Channel::Local(actor_ref), 1.into(), pool_info));

        fn stateful_flatmap_fn<O>(ctx: OperatorContext<O>, x: i32) -> Option<i32>
        where
            O: Operator<OUT = i32>,
        {
            let state = ctx.state_backend;
            let previous_value = state.build("previous_value").value::<i32>();

            // TODO: should all stateful udfs return ArconResult?
            let res = previous_value
                .get(state)
                .expect("Could not get previous value")
                .map(|prev| prev + x);

            previous_value
                .set(state, x)
                .expect("Could not set previous value");

            res
        }

        let stateful_flatmap_node = system.create(move || {
            Node::new(
                String::from("stateful_flatmap_node"),
                0.into(),
                vec![1.into()],
                channel_strategy,
                StatefulFlatMap::new(&stateful_flatmap_fn),
                Box::new(InMemory::new("test".as_ref()).unwrap()),
                timer::none,
            )
        });
        system.start(&stateful_flatmap_node);

        let elem = |x: i32| ArconEvent::Element(ArconElement::new(x));
        let msg = ArconMessage {
            events: vec![
                elem(1).into(),
                elem(2).into(),
                elem(3).into(),
                elem(4).into(),
                ArconEvent::Death(String::from("die")).into(),
            ]
            .into(),
            sender: NodeID::new(1),
        };
        let stateful_flatmap_ref: ActorRefStrong<ArconMessage<i32>> = stateful_flatmap_node
            .actor_ref()
            .hold()
            .expect("failed to fetch");

        stateful_flatmap_ref.tell(msg);

        std::thread::sleep(std::time::Duration::from_secs(1));
        {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(
                comp_inspect.data.iter().map(|x| x.data).collect::<Vec<_>>(),
                vec![3, 5, 7]
            );
        }

        pipeline.shutdown();
    }
}
