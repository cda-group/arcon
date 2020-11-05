// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconElement, ArconNever, ArconType},
    stream::operator::{Operator, OperatorContext},
    util::SafelySendableFn,
};
use arcon_error::*;
use arcon_state::{index::ArconState, Backend};
use kompact::prelude::ComponentDefinition;
use std::marker::PhantomData;

pub struct Filter<IN, F, S, B>
where
    IN: ArconType,
    F: SafelySendableFn(&IN, &mut S) -> bool,
    S: ArconState,
    B: Backend,
{
    state: S,
    udf: F,
    _marker: PhantomData<fn(IN) -> bool>,
    _b: PhantomData<B>,
}

impl<IN, B> Filter<IN, fn(&IN, &mut ()) -> bool, (), B>
where
    IN: ArconType,
    B: Backend,
{
    pub fn new(
        udf: impl SafelySendableFn(&IN) -> bool,
    ) -> Filter<IN, impl SafelySendableFn(&IN, &mut ()) -> bool, (), B> {
        let udf = move |input: &IN, _: &mut ()| udf(input);
        Filter {
            state: (),
            udf,
            _marker: Default::default(),
            _b: PhantomData,
        }
    }
}

impl<IN, F, S, B> Filter<IN, F, S, B>
where
    IN: ArconType,
    F: SafelySendableFn(&IN, &mut S) -> bool,
    S: ArconState,
    B: Backend,
{
    pub fn stateful(state: S, udf: F) -> Self {
        Filter {
            state,
            udf,
            _marker: Default::default(),
            _b: PhantomData,
        }
    }
}

impl<IN, F, S, B> Operator<B> for Filter<IN, F, S, B>
where
    IN: ArconType,
    F: SafelySendableFn(&IN, &mut S) -> bool,
    S: ArconState,
    B: Backend,
{
    type IN = IN;
    type OUT = IN;
    type TimerState = ArconNever;
    type OperatorState = S;

    fn handle_element(
        &mut self,
        element: ArconElement<IN>,
        mut ctx: OperatorContext<Self, B, impl ComponentDefinition>,
    ) -> ArconResult<()> {
        if (self.udf)(&element.data, &mut self.state) {
            ctx.output(element);
        }
        Ok(())
    }
    crate::ignore_timeout!(B);

    fn persist(&mut self) -> Result<(), arcon_state::error::ArconStateError> {
        self.state.persist()
    }
}
