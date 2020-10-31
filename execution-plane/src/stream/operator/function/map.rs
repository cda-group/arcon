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

pub struct Map<IN, OUT, F, S, B>
where
    IN: ArconType,
    OUT: ArconType,
    F: SafelySendableFn(IN, &mut S) -> ArconResult<OUT>,
    S: ArconState,
    B: Backend,
{
    state: S,
    udf: F,
    _marker: PhantomData<fn(IN) -> ArconResult<OUT>>,
    _b: PhantomData<B>,
}

impl<IN, OUT, B> Map<IN, OUT, fn(IN, &mut ()) -> ArconResult<OUT>, (), B>
where
    IN: ArconType,
    OUT: ArconType,
    B: Backend,
{
    pub fn new(
        udf: impl SafelySendableFn(IN) -> ArconResult<OUT>,
    ) -> Map<IN, OUT, impl SafelySendableFn(IN, &mut ()) -> ArconResult<OUT>, (), B> {
        let udf = move |input: IN, _: &mut ()| udf(input);
        Map {
            state: (),
            udf,
            _marker: Default::default(),
            _b: PhantomData,
        }
    }
}

impl<IN, OUT, F, S, B> Map<IN, OUT, F, S, B>
where
    IN: ArconType,
    OUT: ArconType,
    F: SafelySendableFn(IN, &mut S) -> ArconResult<OUT>,
    S: ArconState,
    B: Backend,
{
    pub fn stateful(state: S, udf: F) -> Self {
        Map {
            state,
            udf,
            _marker: Default::default(),
            _b: PhantomData,
        }
    }
}

impl<IN, OUT, F, S, B> Operator<B> for Map<IN, OUT, F, S, B>
where
    IN: ArconType,
    OUT: ArconType,
    F: SafelySendableFn(IN, &mut S) -> ArconResult<OUT>,
    S: ArconState,
    B: Backend,
{
    type IN = IN;
    type OUT = OUT;
    type TimerState = ArconNever;
    type OperatorState = S;

    fn handle_element(
        &mut self,
        element: ArconElement<IN>,
        mut ctx: OperatorContext<Self, B, impl ComponentDefinition>,
    ) -> ArconResult<()> {
        ctx.output(ArconElement {
            data: (self.udf)(element.data, &mut self.state)?,
            timestamp: element.timestamp,
        });

        Ok(())
    }

    crate::ignore_timeout!(B);

    fn persist(&mut self) -> Result<(), arcon_state::error::ArconStateError> {
        self.state.persist()
    }
}
