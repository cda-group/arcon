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

pub struct MapInPlace<IN, F, S, B>
where
    IN: ArconType,
    F: SafelySendableFn(&mut IN, &mut S) -> ArconResult<()>,
    S: ArconState,
    B: Backend,
{
    state: S,
    udf: F,
    _marker: PhantomData<fn(&mut IN) -> ArconResult<()>>,
    _b: PhantomData<B>,
}

impl<IN, B> MapInPlace<IN, fn(&mut IN, &mut ()) -> ArconResult<()>, (), B>
where
    IN: ArconType,
    B: Backend,
{
    pub fn new(
        udf: impl SafelySendableFn(&mut IN) -> ArconResult<()>,
    ) -> MapInPlace<IN, impl SafelySendableFn(&mut IN, &mut ()) -> ArconResult<()>, (), B> {
        let udf = move |input: &mut IN, _: &mut ()| udf(input);
        MapInPlace {
            state: (),
            udf,
            _marker: Default::default(),
            _b: PhantomData,
        }
    }
}

impl<IN, F, S, B> MapInPlace<IN, F, S, B>
where
    IN: ArconType,
    F: SafelySendableFn(&mut IN, &mut S) -> ArconResult<()>,
    S: ArconState,
    B: Backend,
{
    pub fn stateful(state: S, udf: F) -> Self {
        MapInPlace {
            state,
            udf,
            _marker: Default::default(),
            _b: PhantomData,
        }
    }
}

impl<IN, F, S, B> Operator<B> for MapInPlace<IN, F, S, B>
where
    IN: ArconType,
    F: SafelySendableFn(&mut IN, &mut S) -> ArconResult<()>,
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
        let mut elem = element;
        (self.udf)(&mut elem.data, &mut self.state)?;
        ctx.output(elem);
        Ok(())
    }

    crate::ignore_timeout!(B);

    fn persist(&mut self) -> Result<(), arcon_state::error::ArconStateError> {
        self.state.persist()
    }
}
