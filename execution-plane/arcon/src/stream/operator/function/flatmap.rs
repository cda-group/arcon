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

pub struct FlatMap<IN, OUTS, F, B, S>
where
    IN: ArconType,
    OUTS: IntoIterator,
    OUTS::Item: ArconType,
    F: SafelySendableFn(IN, &mut S) -> ArconResult<OUTS>,
    B: Backend,
    S: ArconState,
{
    state: S,
    udf: F,
    _marker: PhantomData<fn(IN) -> ArconResult<OUTS>>,
    _b: PhantomData<B>,
}

impl<IN, OUTS, B> FlatMap<IN, OUTS, fn(IN, &mut ()) -> ArconResult<OUTS>, B, ()>
where
    IN: ArconType,
    OUTS: IntoIterator,
    OUTS::Item: ArconType,
    B: Backend,
{
    pub fn new(
        udf: impl SafelySendableFn(IN) -> ArconResult<OUTS>,
    ) -> FlatMap<IN, OUTS, impl SafelySendableFn(IN, &mut ()) -> ArconResult<OUTS>, B, ()> {
        let udf = move |input: IN, _: &mut ()| udf(input);
        FlatMap {
            state: (),
            udf,
            _marker: Default::default(),
            _b: PhantomData,
        }
    }
}

impl<IN, OUTS, F, B, S> FlatMap<IN, OUTS, F, B, S>
where
    IN: ArconType,
    OUTS: IntoIterator,
    OUTS::Item: ArconType,
    F: SafelySendableFn(IN, &mut S) -> ArconResult<OUTS>,
    S: ArconState,
    B: Backend,
{
    pub fn stateful(state: S, udf: F) -> Self {
        FlatMap {
            state,
            udf,
            _marker: Default::default(),
            _b: PhantomData,
        }
    }
}

impl<IN, OUTS, F, B, S> Operator<B> for FlatMap<IN, OUTS, F, B, S>
where
    IN: ArconType,
    OUTS: IntoIterator,
    OUTS::Item: ArconType,
    F: SafelySendableFn(IN, &mut S) -> ArconResult<OUTS>,
    S: ArconState,
    B: Backend,
{
    type IN = IN;
    type OUT = OUTS::Item;
    type TimerState = ArconNever;
    type OperatorState = S;

    fn handle_element(
        &mut self,
        element: ArconElement<IN>,
        mut ctx: OperatorContext<Self, B, impl ComponentDefinition>,
    ) -> ArconResult<()> {
        let result = (self.udf)(element.data, &mut self.state)?;
        for item in result {
            ctx.output(ArconElement {
                data: item,
                timestamp: element.timestamp,
            });
        }
        Ok(())
    }

    crate::ignore_timeout!(B);

    fn persist(&mut self) -> Result<(), arcon_state::error::ArconStateError> {
        self.state.persist()
    }
}
