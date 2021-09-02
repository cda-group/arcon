use crate::{
    data::{ArconElement, ArconNever, ArconType},
    error::*,
    index::{ArconState, EmptyState},
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
    udf: F,
    _marker: PhantomData<fn(IN, S) -> ArconResult<OUTS>>,
}

impl<IN, OUTS> FlatMap<IN, OUTS, fn(IN, &mut EmptyState) -> ArconResult<OUTS>, EmptyState>
where
    IN: ArconType,
    OUTS: IntoIterator + 'static,
    OUTS::Item: ArconType,
{
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        udf: impl Fn(IN) -> OUTS + ArconFnBounds,
    ) -> FlatMap<
        IN,
        OUTS,
        impl Fn(IN, &mut EmptyState) -> ArconResult<OUTS> + ArconFnBounds,
        EmptyState,
    > {
        let udf = move |input: IN, _: &mut EmptyState| Ok(udf(input));
        FlatMap {
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
    pub fn stateful(udf: F) -> Self {
        FlatMap {
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
        ctx: &mut OperatorContext<Self::TimerState, Self::OperatorState>,
    ) -> ArconResult<Self::ElementIterator> {
        let timestamp = element.timestamp;
        let result = (self.udf)(element.data, ctx.state())?;
        Ok(Box::new(
            result
                .into_iter()
                .map(move |e| ArconElement::with_timestamp(e, timestamp)),
        ))
    }

    crate::ignore_timeout!();
}
