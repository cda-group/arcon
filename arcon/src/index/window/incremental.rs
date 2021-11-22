use crate::data::ArconType;
use crate::stream::operator::window::WindowContext;
use crate::{
    error::ArconResult,
    index::{IndexOps, WindowIndex},
    prelude::*,
    table::ImmutableTable,
    util::{prost_helpers::ProstOption, ArconFnBounds},
};
use arcon_state::{backend::handles::ActiveHandle, Aggregator, AggregatorState, Backend};

#[derive(Clone)]
pub struct IncrementalWindowAggregator<IN, OUT, INIT, AGG>
where
    IN: ArconType,
    OUT: ArconType,
    INIT: Fn(IN) -> OUT + ArconFnBounds,
    AGG: Fn(IN, &OUT) -> OUT + ArconFnBounds,
{
    init: INIT,
    agg: AGG,
    _marker: std::marker::PhantomData<(IN, OUT)>,
}

impl<IN, OUT, INIT, AGG> Aggregator for IncrementalWindowAggregator<IN, OUT, INIT, AGG>
where
    IN: ArconType,
    OUT: ArconType,
    INIT: Fn(IN) -> OUT + ArconFnBounds,
    AGG: Fn(IN, &OUT) -> OUT + ArconFnBounds,
{
    type Input = IN;
    type Accumulator = ProstOption<OUT>; // this should be an option, but prost
    type Result = OUT;

    fn create_accumulator(&self) -> Self::Accumulator {
        None.into()
    }

    fn add(&self, acc: &mut Self::Accumulator, value: IN) {
        match &mut acc.inner {
            None => {
                *acc = Some((self.init)(value)).into();
            }
            Some(inner) => *acc = Some((self.agg)(value, inner)).into(),
        }
    }

    fn merge_accumulators(
        &self,
        _fst: Self::Accumulator,
        _snd: Self::Accumulator,
    ) -> Self::Accumulator {
        unimplemented!()
    }

    fn accumulator_into_result(&self, acc: Self::Accumulator) -> Self::Result {
        let opt: Option<_> = acc.into();
        opt.expect("uninitialized incremental window")
    }
}

// Alias around AggregatorState for IncrementalWindowFn
type AggState<IN, OUT, INIT, AGG> =
    AggregatorState<IncrementalWindowAggregator<IN, OUT, INIT, AGG>>;

/// A window index that incrementally aggregates elements
///
/// Used for associative and commutative operations
///
pub struct IncrementalWindow<IN, OUT, INIT, AGG, B>
where
    IN: ArconType,
    OUT: ArconType,
    INIT: Fn(IN) -> OUT + ArconFnBounds,
    AGG: Fn(IN, &OUT) -> OUT + ArconFnBounds,
    B: Backend,
{
    aggregator: ActiveHandle<B, AggState<IN, OUT, INIT, AGG>, u64, u64>,
}

impl<IN, OUT, INIT, AGG, B> IncrementalWindow<IN, OUT, INIT, AGG, B>
where
    IN: ArconType,
    OUT: ArconType,
    INIT: Fn(IN) -> OUT + ArconFnBounds,
    AGG: Fn(IN, &OUT) -> OUT + ArconFnBounds,
    B: Backend,
{
    pub fn new(backend: Arc<B>, init: INIT, agg: AGG) -> Self {
        let mut aggregator = Handle::aggregator(
            "incremental_window_aggregating_state",
            IncrementalWindowAggregator {
                init,
                agg,
                _marker: std::marker::PhantomData,
            },
        )
        .with_item_key(0)
        .with_namespace(0);

        backend.register_aggregator_handle(&mut aggregator);

        let aggregator = aggregator.activate(backend);

        Self { aggregator }
    }
}

impl<IN, OUT, INIT, AGG, B> WindowIndex for IncrementalWindow<IN, OUT, INIT, AGG, B>
where
    IN: ArconType,
    OUT: ArconType,
    INIT: Fn(IN) -> OUT + ArconFnBounds,
    AGG: Fn(IN, &OUT) -> OUT + ArconFnBounds,
    B: Backend,
{
    type IN = IN;
    type OUT = OUT;
    fn on_element(&mut self, element: Self::IN, ctx: WindowContext) -> ArconResult<()> {
        self.aggregator.set_item_key(ctx.key);
        self.aggregator.set_namespace(ctx.index);

        self.aggregator.aggregate(element)?;

        Ok(())
    }

    fn result(&mut self, ctx: WindowContext) -> ArconResult<Self::OUT> {
        self.aggregator.set_item_key(ctx.key);
        self.aggregator.set_namespace(ctx.index);

        let result = self.aggregator.get()?;
        Ok(result)
    }

    fn clear(&mut self, ctx: WindowContext) -> ArconResult<()> {
        self.aggregator.set_item_key(ctx.key);
        self.aggregator.set_namespace(ctx.index);

        let _ = self.aggregator.clear()?;
        Ok(())
    }
}
impl<IN, OUT, INIT, AGG, B> IndexOps for IncrementalWindow<IN, OUT, INIT, AGG, B>
where
    IN: ArconType,
    OUT: ArconType,
    INIT: Fn(IN) -> OUT + ArconFnBounds,
    AGG: Fn(IN, &OUT) -> OUT + ArconFnBounds,
    B: Backend,
{
    fn persist(&mut self) -> ArconResult<()> {
        Ok(())
    }
    fn set_key(&mut self, _: u64) {}
    fn table(&mut self) -> ArconResult<Option<ImmutableTable>> {
        Ok(None)
    }
}
