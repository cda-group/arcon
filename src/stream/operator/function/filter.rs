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

pub struct Filter<IN, F, S>
where
    IN: ArconType,
    F: Fn(&IN, &mut S) -> bool + ArconFnBounds,
    S: ArconState,
{
    state: S,
    udf: F,
    _marker: PhantomData<fn(IN) -> bool>,
}

impl<IN> Filter<IN, fn(&IN, &mut ()) -> bool, ()>
where
    IN: ArconType,
{
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        udf: impl Fn(&IN) -> bool + ArconFnBounds,
    ) -> Filter<IN, impl Fn(&IN, &mut ()) -> bool + ArconFnBounds, ()> {
        let udf = move |input: &IN, _: &mut ()| udf(input);
        Filter {
            state: (),
            udf,
            _marker: Default::default(),
        }
    }
}

impl<IN, F, S> Filter<IN, F, S>
where
    IN: ArconType,
    F: Fn(&IN, &mut S) -> bool + ArconFnBounds,
    S: ArconState,
{
    pub fn stateful(state: S, udf: F) -> Self {
        Filter {
            state,
            udf,
            _marker: Default::default(),
        }
    }
}

impl<IN, F, S> Operator for Filter<IN, F, S>
where
    IN: ArconType,
    F: Fn(&IN, &mut S) -> bool + ArconFnBounds,
    S: ArconState,
{
    type IN = IN;
    type OUT = IN;
    type TimerState = ArconNever;
    type OperatorState = S;

    fn handle_element(
        &mut self,
        element: ArconElement<IN>,
        mut ctx: OperatorContext<Self, impl Backend, impl ComponentDefinition>,
    ) -> ArconResult<()> {
        if (self.udf)(&element.data, &mut self.state) {
            ctx.output(element);
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
