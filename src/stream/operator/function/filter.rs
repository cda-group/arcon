// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconElement, ArconNever, ArconType},
    error::*,
    index::{ArconState, EmptyState},
    stream::operator::{Operator, OperatorContext},
    util::ArconFnBounds,
};
use std::marker::PhantomData;

pub struct Filter<IN, F, S>
where
    IN: ArconType,
    F: Fn(&IN, &mut S) -> bool + ArconFnBounds,
    S: ArconState,
{
    udf: F,
    _marker: PhantomData<fn(IN, S) -> bool>,
}

impl<IN> Filter<IN, fn(&IN, &mut EmptyState) -> bool, EmptyState>
where
    IN: ArconType,
{
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        udf: impl Fn(&IN) -> bool + ArconFnBounds,
    ) -> Filter<IN, impl Fn(&IN, &mut EmptyState) -> bool + ArconFnBounds, EmptyState> {
        let udf = move |input: &IN, _: &mut EmptyState| udf(input);
        Filter {
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
    pub fn stateful(udf: F) -> Self {
        Filter {
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
    type ElementIterator = Option<ArconElement<Self::OUT>>;

    fn handle_element(
        &mut self,
        element: ArconElement<IN>,
        ctx: &mut OperatorContext<Self::TimerState, Self::OperatorState>,
    ) -> ArconResult<Self::ElementIterator> {
        if (self.udf)(&element.data, ctx.state()) {
            Ok(Some(element))
        } else {
            Ok(None)
        }
    }
    crate::ignore_timeout!();
}
