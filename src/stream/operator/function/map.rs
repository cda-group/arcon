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

pub struct Map<IN, OUT, F, S>
where
    IN: ArconType,
    OUT: ArconType,
    F: SafelySendableFn(IN, &mut S) -> OperatorResult<OUT>,
    S: ArconState,
{
    state: S,
    udf: F,
    _marker: PhantomData<fn(IN) -> OperatorResult<OUT>>,
}

impl<IN, OUT> Map<IN, OUT, fn(IN, &mut ()) -> OperatorResult<OUT>, ()>
where
    IN: ArconType,
    OUT: ArconType,
{
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        udf: impl SafelySendableFn(IN) -> OUT,
    ) -> Map<IN, OUT, impl SafelySendableFn(IN, &mut ()) -> OperatorResult<OUT>, ()> {
        let udf = move |input: IN, _: &mut ()| {
            let output = udf(input);
            Ok(output)
        };

        Map {
            state: (),
            udf,
            _marker: Default::default(),
        }
    }
}

impl<IN, OUT, F, S> Map<IN, OUT, F, S>
where
    IN: ArconType,
    OUT: ArconType,
    F: SafelySendableFn(IN, &mut S) -> OperatorResult<OUT>,
    S: ArconState,
{
    pub fn stateful(state: S, udf: F) -> Self {
        Map {
            state,
            udf,
            _marker: Default::default(),
        }
    }
}

impl<IN, OUT, F, S> Operator for Map<IN, OUT, F, S>
where
    IN: ArconType,
    OUT: ArconType,
    F: SafelySendableFn(IN, &mut S) -> OperatorResult<OUT>,
    S: ArconState,
{
    type IN = IN;
    type OUT = OUT;
    type TimerState = ArconNever;
    type OperatorState = S;

    fn handle_element(
        &mut self,
        element: ArconElement<IN>,
        mut ctx: OperatorContext<Self, impl Backend, impl ComponentDefinition>,
    ) -> OperatorResult<()> {
        ctx.output(ArconElement {
            data: (self.udf)(element.data, &mut self.state)?,
            timestamp: element.timestamp,
        });

        Ok(())
    }

    crate::ignore_timeout!();

    fn persist(&mut self) -> Result<(), arcon_state::error::ArconStateError> {
        self.state.persist()
    }
}
