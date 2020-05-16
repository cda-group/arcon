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

/// IN: input event
/// OUTS: output events (a collection-like type)
/// F: closure type
/// B: state backend type
/// S: state bundle type
pub struct FlatMap<IN, OUTS, F, B, S>
where
    IN: ArconType,
    OUTS: IntoIterator,
    OUTS::Item: ArconType,
    F: SafelySendableFn(IN, &S, &mut state::Session<B>) -> OUTS,
    B: state::Backend,
    S: state::GenericBundle<B>,
{
    state: S,
    udf: F,
    _marker: PhantomData<fn(IN, B) -> OUTS>,
}

impl<IN, OUTS, B> FlatMap<IN, OUTS, fn(IN, &(), &mut state::Session<B>) -> OUTS, B, ()>
where
    IN: ArconType,
    OUTS: IntoIterator,
    OUTS::Item: ArconType,
    B: state::Backend,
{
    pub fn new(
        udf: impl SafelySendableFn(IN) -> OUTS,
    ) -> FlatMap<IN, OUTS, impl SafelySendableFn(IN, &(), &mut state::Session<B>) -> OUTS, B, ()>
    {
        let udf = move |input: IN, _: &(), _: &mut state::Session<B>| udf(input);
        FlatMap {
            state: (),
            udf,
            _marker: Default::default(),
        }
    }
}

impl<IN, OUTS, F, B, S> FlatMap<IN, OUTS, F, B, S>
where
    IN: ArconType,
    OUTS: IntoIterator,
    OUTS::Item: ArconType,
    F: SafelySendableFn(IN, &S, &mut state::Session<B>) -> OUTS,
    B: state::Backend,
    S: state::GenericBundle<B>,
{
    pub fn stateful(state: S, udf: F) -> Self {
        FlatMap {
            state,
            udf,
            _marker: Default::default(),
        }
    }
}

impl<IN, OUTS, F, B, S> Operator<B> for FlatMap<IN, OUTS, F, B, S>
where
    IN: ArconType,
    OUTS: IntoIterator,
    OUTS::Item: ArconType,
    F: SafelySendableFn(IN, &S, &mut state::Session<B>) -> OUTS,
    B: state::Backend,
    S: state::GenericBundle<B>,
{
    type IN = IN;
    type OUT = OUTS::Item;
    type TimerState = ArconNever;

    fn register_states(&mut self, registration_token: &mut state::RegistrationToken<B>) {
        self.state.register_states(registration_token);
    }

    fn init(&mut self, _session: &mut state::Session<B>) {}

    fn handle_element(
        &self,
        element: ArconElement<IN>,
        mut ctx: OperatorContext<Self, B, impl TimerBackend<Self::TimerState>>,
    ) {
        let result = (self.udf)(element.data, &self.state, ctx.state_session);
        for item in result {
            ctx.output(ArconEvent::Element(ArconElement {
                data: item,
                timestamp: element.timestamp,
            }));
        }
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
    use crate::{prelude::*, state::InMemory, timer};

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

        fn flatmap_fn(x: i32) -> Vec<i32> {
            (0..x).map(|x| x + 5).collect()
        }
        let flatmap_node = system.create(move || {
            Node::new(
                String::from("flatmap_node"),
                0.into(),
                vec![1.into()],
                channel_strategy,
                FlatMap::new(&flatmap_fn),
                InMemory::create("test".as_ref()).unwrap(),
                timer::none(),
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

    arcon_state::bundle! {
        struct TestState {
            value: Handle<ValueState<i32>>
        }
    }

    #[test]
    fn stateful_flat_map_test() {
        let mut pipeline = ArconPipeline::new();
        let pool_info = pipeline.get_pool_info();
        let system = pipeline.system();

        let comp = system.create(move || DebugNode::<i32>::new());
        system.start(&comp);

        let actor_ref: ActorRefStrong<ArconMessage<i32>> =
            comp.actor_ref().hold().expect("failed to fetch");
        let channel_strategy =
            ChannelStrategy::Forward(Forward::new(Channel::Local(actor_ref), 1.into(), pool_info));

        fn flatmap_fn(x: i32, state: &TestState, sess: &mut state::Session<InMemory>) -> Vec<i32> {
            let mut state = state.activate(sess);
            let res = (0..x)
                .map(|x| x + state.value().get().unwrap().unwrap_or(0))
                .collect();
            state.value().set(x).unwrap();
            res
        }

        let flatmap_node = system.create(move || {
            Node::new(
                String::from("flatmap_node"),
                0.into(),
                vec![1.into()],
                channel_strategy,
                FlatMap::stateful(
                    TestState {
                        value: Handle::value("test"),
                    },
                    &flatmap_fn,
                ),
                InMemory::create("test".as_ref()).unwrap(),
                timer::none(),
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
