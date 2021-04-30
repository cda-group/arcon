// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

pub mod assigner;

pub use assigner::WindowAssigner;

use crate::{
    prelude::*,
    util::{prost_helpers::ProstOption, SafelySendableFn},
};
use arcon_error::OperatorResult;
use arcon_state::{backend::handles::ActiveHandle, Aggregator, AggregatorState, Backend, VecState};
use fxhash::FxHasher;
use std::hash::{Hash, Hasher};

#[derive(prost::Message, Hash, Copy, Clone)]
pub struct WindowContext {
    #[prost(uint64)]
    key: u64,
    #[prost(uint64)]
    index: u64,
}

impl WindowContext {
    pub fn new(key: u64, index: u64) -> Self {
        WindowContext { key, index }
    }
}

impl From<WindowContext> for u64 {
    fn from(ctx: WindowContext) -> Self {
        let mut s = FxHasher::default();
        ctx.hash(&mut s);
        s.finish()
    }
}

/// `Window` consists of the methods required by each window implementation
///
/// IN: Element type sent to the Window
/// OUT: Expected output type of the Window
pub trait Window<IN, OUT>: Send
where
    IN: ArconType,
    OUT: ArconType,
{
    /// The `on_element` function is called per received window element
    fn on_element(&mut self, element: IN, ctx: WindowContext) -> OperatorResult<()>;
    /// The `result` function is called at the end of a window's lifetime
    fn result(&mut self, ctx: WindowContext) -> OperatorResult<OUT>;
    /// Clears the window state for the passed context
    fn clear(&mut self, ctx: WindowContext) -> OperatorResult<()>;
}

pub struct AppenderWindow<IN, OUT, B>
where
    IN: ArconType,
    OUT: ArconType,
    B: Backend,
{
    handle: ActiveHandle<B, VecState<IN>, u64, u64>,
    materializer: &'static dyn SafelySendableFn(&[IN]) -> OUT,
}

impl<IN, OUT, B> AppenderWindow<IN, OUT, B>
where
    IN: ArconType,
    OUT: ArconType,
    B: Backend,
{
    pub fn new(
        backend: Arc<B>,
        materializer: &'static dyn SafelySendableFn(&[IN]) -> OUT,
    ) -> AppenderWindow<IN, OUT, B> {
        let mut handle = Handle::vec("window_handle")
            .with_item_key(0)
            .with_namespace(0);

        backend.register_vec_handle(&mut handle);

        let handle = handle.activate(backend);

        AppenderWindow {
            handle,
            materializer,
        }
    }
}

impl<IN, OUT, B> Window<IN, OUT> for AppenderWindow<IN, OUT, B>
where
    IN: ArconType,
    OUT: ArconType,
    B: Backend,
{
    fn on_element(&mut self, element: IN, ctx: WindowContext) -> OperatorResult<()> {
        self.handle.set_item_key(ctx.key);
        self.handle.set_namespace(ctx.index);

        self.handle.append(element)?;
        Ok(())
    }

    fn result(&mut self, ctx: WindowContext) -> OperatorResult<OUT> {
        self.handle.set_item_key(ctx.key);
        self.handle.set_namespace(ctx.index);

        let buf = self.handle.get()?;
        Ok((self.materializer)(&buf))
    }

    fn clear(&mut self, ctx: WindowContext) -> OperatorResult<()> {
        self.handle.set_item_key(ctx.key);
        self.handle.set_namespace(ctx.index);

        self.handle.clear()?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct IncrementalWindowAggregator<IN: ArconType, OUT: ArconType>(
    &'static dyn SafelySendableFn(IN) -> OUT,
    &'static dyn SafelySendableFn(IN, &OUT) -> OUT,
);

impl<IN: ArconType, OUT: ArconType> Aggregator for IncrementalWindowAggregator<IN, OUT> {
    type Input = IN;
    type Accumulator = ProstOption<OUT>; // this should be an option, but prost
    type Result = OUT;

    fn create_accumulator(&self) -> Self::Accumulator {
        None.into()
    }

    fn add(&self, acc: &mut Self::Accumulator, value: IN) {
        match &mut acc.inner {
            None => {
                *acc = Some((self.0)(value)).into();
            }
            Some(inner) => *acc = Some((self.1)(value, inner)).into(),
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

pub struct IncrementalWindow<IN, OUT, B>
where
    IN: ArconType,
    OUT: ArconType,
    B: Backend,
{
    aggregator: ActiveHandle<B, AggregatorState<IncrementalWindowAggregator<IN, OUT>>, u64, u64>,
}

impl<IN, OUT, B> IncrementalWindow<IN, OUT, B>
where
    IN: ArconType,
    OUT: ArconType,
    B: Backend,
{
    pub fn new(
        backend: Arc<B>,
        init: &'static dyn SafelySendableFn(IN) -> OUT,
        agg: &'static dyn SafelySendableFn(IN, &OUT) -> OUT,
    ) -> IncrementalWindow<IN, OUT, B> {
        let mut aggregator = Handle::aggregator(
            "incremental_window_aggregating_state",
            IncrementalWindowAggregator(init, agg),
        )
        .with_item_key(0)
        .with_namespace(0);

        backend.register_aggregator_handle(&mut aggregator);

        let aggregator = aggregator.activate(backend);

        IncrementalWindow { aggregator }
    }
}

impl<IN, OUT, B> Window<IN, OUT> for IncrementalWindow<IN, OUT, B>
where
    IN: ArconType,
    OUT: ArconType,
    B: Backend,
{
    fn on_element(&mut self, element: IN, ctx: WindowContext) -> OperatorResult<()> {
        self.aggregator.set_item_key(ctx.key);
        self.aggregator.set_namespace(ctx.index);

        self.aggregator.aggregate(element)?;

        Ok(())
    }

    fn result(&mut self, ctx: WindowContext) -> OperatorResult<OUT> {
        self.aggregator.set_item_key(ctx.key);
        self.aggregator.set_namespace(ctx.index);

        self.aggregator.get()
    }

    fn clear(&mut self, ctx: WindowContext) -> OperatorResult<()> {
        self.aggregator.set_item_key(ctx.key);
        self.aggregator.set_namespace(ctx.index);

        self.aggregator.clear()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::temp_backend;
    use std::sync::Arc;

    #[test]
    fn sum_appender_window_test() {
        let backend = Arc::new(temp_backend());

        fn materializer(buffer: &[i32]) -> i32 {
            buffer.iter().sum()
        }

        let mut window = AppenderWindow::new(backend, &materializer);

        for i in 0..10 {
            let _ = window.on_element(i, WindowContext::new(0, 0));
        }

        let sum = window.result(WindowContext::new(0, 0)).unwrap();

        let expected: i32 = 45;
        assert_eq!(sum, expected);
    }

    #[test]
    fn sum_incremental_window_test() {
        let backend = Arc::new(temp_backend());

        fn init(i: i32) -> u64 {
            i as u64
        }
        fn aggregation(i: i32, agg: &u64) -> u64 {
            agg + i as u64
        }

        let mut window = IncrementalWindow::new(backend, &init, &aggregation);

        for i in 0..10 {
            let _ = window.on_element(i, WindowContext::new(0, 0));
        }

        for i in 0..20 {
            let _ = window.on_element(i, WindowContext::new(1, 1));
        }

        let sum_one = window.result(WindowContext::new(0, 0)).unwrap();
        assert_eq!(sum_one, 45);
        let sum_two = window.result(WindowContext::new(1, 1)).unwrap();
        assert_eq!(sum_two, 190);
    }
}
