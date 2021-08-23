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

pub struct MapInPlace<IN, F, S>
where
    IN: ArconType,
    F: Fn(&mut IN, &mut S) -> ArconResult<()> + ArconFnBounds,
    S: ArconState,
{
    udf: F,
    _marker: PhantomData<fn(&mut IN, S) -> ArconResult<()>>,
}

impl<IN> MapInPlace<IN, fn(&mut IN, &mut EmptyState) -> ArconResult<()>, EmptyState>
where
    IN: ArconType,
{
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        udf: impl Fn(&mut IN) + ArconFnBounds,
    ) -> MapInPlace<
        IN,
        impl Fn(&mut IN, &mut EmptyState) -> ArconResult<()> + ArconFnBounds,
        EmptyState,
    > {
        let udf = move |input: &mut IN, _: &mut EmptyState| {
            udf(input);
            Ok(())
        };
        MapInPlace {
            udf,
            _marker: Default::default(),
        }
    }
}

impl<IN, F, S> MapInPlace<IN, F, S>
where
    IN: ArconType,
    F: Fn(&mut IN, &mut S) -> ArconResult<()> + ArconFnBounds,
    S: ArconState,
{
    pub fn stateful(udf: F) -> Self {
        MapInPlace {
            udf,
            _marker: Default::default(),
        }
    }
}

impl<IN, F, S> Operator for MapInPlace<IN, F, S>
where
    IN: ArconType,
    F: Fn(&mut IN, &mut S) -> ArconResult<()> + ArconFnBounds,
    S: ArconState,
{
    type IN = IN;
    type OUT = IN;
    type TimerState = ArconNever;
    type OperatorState = S;
    type ElementIterator = std::iter::Once<ArconElement<Self::OUT>>;

    fn handle_element(
        &mut self,
        element: ArconElement<IN>,
        ctx: &mut OperatorContext<Self::TimerState, Self::OperatorState>,
    ) -> ArconResult<Self::ElementIterator> {
        let mut elem = element;
        (self.udf)(&mut elem.data, ctx.state())?;
        Ok(std::iter::once(elem))
    }

    crate::ignore_timeout!();
}
