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
/// F: closure type
/// B: state backend type
/// S: state bundle type
// I would _love_ for Filter not to have B as a parameter, but that would require like, at least
// GATs and maybe even generic closures. If we don't want any dynamic dispatch that is.
pub struct Filter<IN, F, B, S>
where
    IN: ArconType,
    F: SafelySendableFn(&IN, &S, &mut state::Session<B>) -> bool,
    B: state::Backend,
    S: state::GenericBundle<B>,
{
    state: S,
    udf: F,
    // phantom data of fn, because we don't care if captured generics aren't Send
    _marker: PhantomData<fn(IN, B)>,
}

impl<IN, B> Filter<IN, fn(&IN, &(), &mut state::Session<B>) -> bool, B, ()>
where
    IN: ArconType,
    B: state::Backend,
{
    pub fn new(
        udf: impl SafelySendableFn(&IN) -> bool,
    ) -> Filter<IN, impl SafelySendableFn(&IN, &(), &mut state::Session<B>) -> bool, B, ()> {
        let udf = move |input: &IN, _: &(), _: &mut state::Session<B>| udf(input);
        Filter {
            state: (),
            udf,
            _marker: Default::default(),
        }
    }
}

impl<IN, F, B, S> Filter<IN, F, B, S>
where
    IN: ArconType,
    B: state::Backend,
    S: state::GenericBundle<B>,
    F: SafelySendableFn(&IN, &S, &mut state::Session<B>) -> bool,
{
    pub fn stateful(state: S, udf: F) -> Self {
        Filter {
            state,
            udf,
            _marker: Default::default(),
        }
    }
}

impl<IN, B, S, F> Operator<B> for Filter<IN, F, B, S>
where
    IN: ArconType,
    B: state::Backend,
    S: state::GenericBundle<B>,
    F: SafelySendableFn(&IN, &S, &mut state::Session<B>) -> bool,
{
    type IN = IN;
    type OUT = IN;
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
        let Filter { state, udf, .. } = self;

        // the first thing the udf will pretty much always do is to activate the state
        // we cannot do that out here, because rustc's buggy
        // https://github.com/rust-lang/rust/issues/62529
        if udf(&element.data, state, ctx.state_session) {
            ctx.output(ArconEvent::Element(element));
        }
    }

    fn handle_watermark(
        &self,
        _w: Watermark,
        _ctx: OperatorContext<Self, B, impl TimerBackend<Self::TimerState>>,
    ) -> () {
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
    use crate::{prelude::*, state::InMemory};

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
                InMemory::create("test".as_ref()).unwrap(),
                timer::none(),
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
