// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconElement, ArconNever, ArconType},
    stream::operator::{Operator, OperatorContext},
    util::ArconFnBounds,
};
use arcon_error::*;
use arcon_state::{index::ArconState, Backend};
use kompact::prelude::ComponentDefinition;
use std::marker::PhantomData;

pub struct MapInPlace<IN, F, S>
where
    IN: ArconType,
    F: Fn(&mut IN, &mut S) -> OperatorResult<()> + ArconFnBounds,
    S: ArconState,
{
    state: S,
    udf: F,
    _marker: PhantomData<fn(&mut IN) -> OperatorResult<()>>,
}

impl<IN> MapInPlace<IN, fn(&mut IN, &mut ()) -> OperatorResult<()>, ()>
where
    IN: ArconType,
{
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        udf: impl Fn(&mut IN) + ArconFnBounds,
    ) -> MapInPlace<IN, impl Fn(&mut IN, &mut ()) -> OperatorResult<()> + ArconFnBounds, ()> {
        let udf = move |input: &mut IN, _: &mut ()| {
            udf(input);
            Ok(())
        };
        MapInPlace {
            state: (),
            udf,
            _marker: Default::default(),
        }
    }
}

impl<IN, F, S> MapInPlace<IN, F, S>
where
    IN: ArconType,
    F: Fn(&mut IN, &mut S) -> OperatorResult<()> + ArconFnBounds,
    S: ArconState,
{
    pub fn stateful(state: S, udf: F) -> Self {
        MapInPlace {
            state,
            udf,
            _marker: Default::default(),
        }
    }
}

impl<IN, F, S> Operator for MapInPlace<IN, F, S>
where
    IN: ArconType,
    F: Fn(&mut IN, &mut S) -> OperatorResult<()> + ArconFnBounds,
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
    ) -> OperatorResult<()> {
        let mut elem = element;
        (self.udf)(&mut elem.data, &mut self.state)?;
        ctx.output(elem);
        Ok(())
    }

    crate::ignore_timeout!();

    fn persist(&mut self) -> Result<(), arcon_state::error::ArconStateError> {
        self.state.persist()
    }
}
