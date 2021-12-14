use crate::{
    data::{ArconElement, ArconNever, ArconType},
    error::*,
    index::{ArconState, EmptyState},
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
    udf: F,
    _marker: PhantomData<fn(IN, S) -> ArconResult<OUT>>,
}

impl<IN, OUT> Map<IN, OUT, fn(IN, &mut EmptyState) -> ArconResult<OUT>, EmptyState>
where
    IN: ArconType,
    OUT: ArconType,
{
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        udf: impl Fn(IN) -> OUT + ArconFnBounds,
    ) -> Map<IN, OUT, impl Fn(IN, &mut EmptyState) -> ArconResult<OUT> + ArconFnBounds, EmptyState>
    {
        let udf = move |input: IN, _: &mut EmptyState| {
            let output = udf(input);
            Ok(output)
        };

        Map {
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
    pub fn stateful(udf: F) -> Self {
        Map {
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
        ctx: &mut OperatorContext<Self::TimerState, Self::OperatorState>,
    ) -> ArconResult<Self::ElementIterator> {
        let data = (self.udf)(element.data, ctx.state())?;
        Ok(std::iter::once(ArconElement::with_timestamp(
            data,
            element.timestamp,
        )))
    }

    crate::ignore_timeout!();
}
