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
    OUTS: IntoIterator + 'static,
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
    OUTS: IntoIterator + 'static,
    OUTS::Item: ArconType,
    F: Fn(IN, &mut S) -> ArconResult<OUTS> + ArconFnBounds,
    S: ArconState,
{
    type IN = IN;
    type OUT = OUTS::Item;
    type TimerState = ArconNever;
    type OperatorState = S;
    type ElementIterator = Box<dyn Iterator<Item = ArconElement<Self::OUT>>>;

    fn handle_element(
        &mut self,
        element: ArconElement<IN>,
        _: OperatorContext<Self>,
    ) -> ArconResult<Self::ElementIterator> {
        let timestamp = element.timestamp;
        let result = (self.udf)(element.data, &mut self.state)?;
        Ok(Box::new(
            result
                .into_iter()
                .map(move |e| ArconElement::with_timestamp(e, timestamp)),
        ))
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
