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

/// An Arcon operator for performing an in-place map
///
/// IN: Input Event
pub struct MapInPlace<IN, F, B, S>
where
    IN: ArconType,
    F: SafelySendableFn(&mut IN, &S, &mut state::Session<B>),
    B: state::Backend,
    S: state::GenericBundle<B>,
{
    state: S,
    udf: F,
    _marker: PhantomData<fn(IN, B)>,
}

impl<IN, B> MapInPlace<IN, fn(&mut IN, &(), &mut state::Session<B>), B, ()>
where
    IN: ArconType,
    B: state::Backend,
{
    pub fn new(
        udf: impl SafelySendableFn(&mut IN),
    ) -> MapInPlace<IN, impl SafelySendableFn(&mut IN, &(), &mut state::Session<B>), B, ()> {
        let udf = move |input: &mut IN, _: &(), _: &mut state::Session<B>| udf(input);
        MapInPlace {
            state: (),
            udf,
            _marker: Default::default(),
        }
    }
}

impl<IN, F, B, S> MapInPlace<IN, F, B, S>
where
    IN: ArconType,
    F: SafelySendableFn(&mut IN, &S, &mut state::Session<B>),
    B: state::Backend,
    S: state::GenericBundle<B>,
{
    pub fn stateful(state: S, udf: F) -> Self {
        MapInPlace {
            state,
            udf,
            _marker: Default::default(),
        }
    }
}

impl<IN, F, B, S> Operator<B> for MapInPlace<IN, F, B, S>
where
    IN: ArconType,
    F: SafelySendableFn(&mut IN, &S, &mut state::Session<B>),
    B: state::Backend,
    S: state::GenericBundle<B>,
{
    type IN = IN;
    type OUT = IN;
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
        let mut elem = element;
        (self.udf)(&mut elem.data, &self.state, ctx.state_session);
        ctx.output(ArconEvent::Element(elem));
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
