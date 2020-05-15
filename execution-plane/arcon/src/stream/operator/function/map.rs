// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconElement, ArconEvent, ArconNever, ArconType, Epoch, Watermark},
    prelude::state,
    stream::operator::{Operator, OperatorContext},
    timer::TimerBackend,
    util::SafelySendableFn,
};
use std::marker::PhantomData;

/// IN: Input Event
/// OUT: Output Event
/// F: closure type
/// B: state backend type
/// S: state type
pub struct Map<IN, OUT, F, B, S>
where
    IN: ArconType,
    OUT: ArconType,
    F: SafelySendableFn(IN, &S, &mut state::Session<B>) -> OUT,
    B: state::Backend,
    S: state::GenericBundle<B>,
{
    state: S,
    udf: F,
    _marker: PhantomData<fn(IN, B) -> OUT>,
}

impl<IN, OUT, F, B, S> Map<IN, OUT, F, B, S>
where
    IN: ArconType,
    OUT: ArconType,
    F: SafelySendableFn(IN, &S, &mut state::Session<B>) -> OUT,
    B: state::Backend,
    S: state::GenericBundle<B>,
{
    pub fn stateful(state: S, udf: F) -> Self {
        Map {
            state,
            udf,
            _marker: Default::default(),
        }
    }

    pub fn new<FF>(
        udf: impl SafelySendableFn(IN) -> OUT,
    ) -> Map<IN, OUT, impl SafelySendableFn(IN, &(), &mut state::Session<B>) -> OUT, B, ()> {
        let udf = move |input: IN, _: &(), _: &mut state::Session<B>| udf(input);
        Map {
            state: (),
            udf,
            _marker: Default::default(),
        }
    }
}

impl<IN, OUT, F, B, S> Operator<B> for Map<IN, OUT, F, B, S>
where
    IN: ArconType,
    OUT: ArconType,
    F: SafelySendableFn(IN, &S, &mut state::Session<B>) -> OUT,
    B: state::Backend,
    S: state::GenericBundle<B>,
{
    type IN = IN;
    type OUT = OUT;
    type TimerState = ArconNever;

    fn register_states(&mut self, registration_token: &mut state::RegistrationToken<B>) {
        self.state.register_states(registration_token)
    }

    fn init(&mut self, _session: &mut state::Session<B>) {}

    fn handle_element(
        &self,
        element: ArconElement<IN>,
        mut ctx: OperatorContext<Self, B, impl TimerBackend<Self::TimerState>>,
    ) {
        let result = (self.udf)(element.data, &self.state, ctx.state_session);
        let out_elem = ArconElement {
            data: result,
            timestamp: element.timestamp,
        };
        ctx.output(ArconEvent::Element(out_elem));
    }

    fn handle_watermark(
        &self,
        _w: Watermark,
        _ctx: OperatorContext<Self, B, impl TimerBackend<Self::TimerState>>,
    ) {
    }

    fn handle_epoch(
        &self,
        _epoch: Epoch,
        _ctx: OperatorContext<Self, B, impl TimerBackend<Self::TimerState>>,
    ) {
    }

    fn handle_timeout(
        &self,
        _timeout: Self::TimerState,
        _ctx: OperatorContext<Self, B, impl TimerBackend<Self::TimerState>>,
    ) {
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{prelude::*, state_backend::in_memory::InMemory, timer};

    #[test]
    fn map_test() {
        let mut pipeline = ArconPipeline::new();
        let pool_info = pipeline.get_pool_info();
        let system = pipeline.system();

        let comp = system.create(move || DebugNode::<i32>::new());
        system.start(&comp);

        let actor_ref: ActorRefStrong<ArconMessage<i32>> =
            comp.actor_ref().hold().expect("failed to fetch");
        let channel_strategy =
            ChannelStrategy::Forward(Forward::new(Channel::Local(actor_ref), 1.into(), pool_info));
        fn map_fn(x: i32) -> i32 {
            x + 10
        }

        let map_node = system.create(move || {
            Node::new(
                String::from("map_node"),
                0.into(),
                vec![1.into()],
                channel_strategy,
                Map::new(&map_fn),
                Box::new(InMemory::new("test".as_ref()).unwrap()),
                timer::none,
            )
        });
        system.start(&map_node);

        let input_one = ArconEvent::Element(ArconElement::new(6 as i32));
        let input_two = ArconEvent::Element(ArconElement::new(7 as i32));
        let msg = ArconMessage {
            events: vec![
                input_one.into(),
                input_two.into(),
                ArconEvent::Death("die".into()).into(),
            ]
            .into(),
            sender: NodeID::new(1),
        };
        let map_ref: ActorRefStrong<ArconMessage<i32>> =
            map_node.actor_ref().hold().expect("failed to fetch");

        map_ref.tell(msg);

        std::thread::sleep(std::time::Duration::from_secs(1));
        {
            let comp_inspect = &comp.definition().lock().unwrap();
            assert_eq!(comp_inspect.data.len(), 2);
            assert_eq!(comp_inspect.data[0].data, 16);
            assert_eq!(comp_inspect.data[1].data, 17);
        }
        pipeline.shutdown();
    }
}
