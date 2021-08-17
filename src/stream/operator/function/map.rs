// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconElement, ArconNever, ArconType},
    error::*,
    index::ArconState,
    stream::operator::{Operator, OperatorContext},
    util::ArconFnBounds,
};
use std::marker::PhantomData;

pub struct Map<IN, OUT, F, S>
where
    IN: ArconType,
    OUT: ArconType,
    F: Fn(IN, &mut S) -> ArconResult<OUT> + ArconFnBounds,
    S: ArconState,
{
    state: S,
    udf: F,
    _marker: PhantomData<fn(IN) -> ArconResult<OUT>>,
}

impl<IN, OUT> Map<IN, OUT, fn(IN, &mut ()) -> ArconResult<OUT>, ()>
where
    IN: ArconType,
    OUT: ArconType,
{
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        udf: impl Fn(IN) -> OUT + ArconFnBounds,
    ) -> Map<IN, OUT, impl Fn(IN, &mut ()) -> ArconResult<OUT> + ArconFnBounds, ()> {
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
    F: Fn(IN, &mut S) -> ArconResult<OUT> + ArconFnBounds,
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
    F: Fn(IN, &mut S) -> ArconResult<OUT> + ArconFnBounds,
    S: ArconState,
{
    type IN = IN;
    type OUT = OUT;
    type TimerState = ArconNever;
    type OperatorState = S;
    type ElementIterator = std::iter::Once<ArconElement<Self::OUT>>;

    fn handle_element(
        &mut self,
        element: ArconElement<IN>,
        _: OperatorContext<Self>,
    ) -> ArconResult<Self::ElementIterator> {
        let data = (self.udf)(element.data, &mut self.state)?;
        Ok(std::iter::once(ArconElement::with_timestamp(
            data,
            element.timestamp,
        )))
    }

    crate::ignore_timeout!();

    fn persist(&mut self) -> ArconResult<()> {
        self.state.persist()?;
        Ok(())
    }
    fn state(&mut self) -> &mut Self::OperatorState {
        &mut self.state
    }
}
