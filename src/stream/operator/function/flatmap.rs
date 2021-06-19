// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconElement, ArconNever, ArconType},
    error::*,
    index::ArconState,
    stream::operator::{Operator, OperatorContext},
    util::ArconFnBounds,
};
use arcon_state::Backend;
use kompact::prelude::ComponentDefinition;
use std::marker::PhantomData;

pub struct FlatMap<IN, OUTS, F, S>
where
    IN: ArconType,
    OUTS: IntoIterator,
    OUTS::Item: ArconType,
    F: Fn(IN, &mut S) -> ArconResult<OUTS> + ArconFnBounds,
    S: ArconState,
{
    state: S,
    udf: F,
    _marker: PhantomData<fn(IN) -> ArconResult<OUTS>>,
}

impl<IN, OUTS> FlatMap<IN, OUTS, fn(IN, &mut ()) -> ArconResult<OUTS>, ()>
where
    IN: ArconType,
    OUTS: IntoIterator + 'static,
    OUTS::Item: ArconType,
{
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        udf: impl Fn(IN) -> OUTS + ArconFnBounds,
    ) -> FlatMap<IN, OUTS, impl Fn(IN, &mut ()) -> ArconResult<OUTS> + ArconFnBounds, ()> {
        let udf = move |input: IN, _: &mut ()| Ok(udf(input));
        FlatMap {
            state: (),
            udf,
            _marker: Default::default(),
        }
    }
}

impl<IN, OUTS, F, S> FlatMap<IN, OUTS, F, S>
where
    IN: ArconType,
    OUTS: IntoIterator,
    OUTS::Item: ArconType,
    F: Fn(IN, &mut S) -> ArconResult<OUTS> + ArconFnBounds,
    S: ArconState,
{
    pub fn stateful(state: S, udf: F) -> Self {
        FlatMap {
            state,
            udf,
            _marker: Default::default(),
        }
    }
}

impl<IN, OUTS, F, S> Operator for FlatMap<IN, OUTS, F, S>
where
    IN: ArconType,
    OUTS: IntoIterator,
    OUTS::Item: ArconType,
    F: Fn(IN, &mut S) -> ArconResult<OUTS> + ArconFnBounds,
    S: ArconState,
{
    type IN = IN;
    type OUT = OUTS::Item;
    type TimerState = ArconNever;
    type OperatorState = S;

    fn handle_element(
        &mut self,
        element: ArconElement<IN>,
        mut ctx: OperatorContext<Self, impl Backend, impl ComponentDefinition>,
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

    crate::ignore_timeout!();

    fn persist(&mut self) -> Result<(), arcon_state::error::ArconStateError> {
        self.state.persist()
    }
    fn state(&mut self) -> &mut Self::OperatorState {
        &mut self.state
    }
}
