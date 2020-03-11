// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

pub mod event_time;

pub use event_time::EventTimeWindowAssigner;

use crate::{
    prelude::*,
    util::{prost_helpers::ProstOption, SafelySendableFn},
};

pub struct WindowContext<'s> {
    state_backend: &'s mut dyn StateBackend,
    key: u64,
    index: u64,
}

impl<'s> WindowContext<'s> {
    pub fn new(state_backend: &'s mut dyn StateBackend, key: u64, index: u64) -> Self {
        WindowContext {
            state_backend,
            key,
            index,
        }
    }
}

/// `Window` consists of the methods required by each window implementation
///
/// IN: Element type sent to the Window
/// OUT: Expected output type of the Window
pub trait Window<IN, OUT>: Send + Sync
where
    IN: ArconType,
    OUT: ArconType,
{
    /// The `on_element` function is called per received window element
    fn on_element(&mut self, element: IN, ctx: WindowContext) -> ArconResult<()>;
    /// The `result` function is called at the end of a window's lifetime
    fn result(&mut self, ctx: WindowContext) -> ArconResult<OUT>;
    /// Clears the window state for the passed context
    fn clear(&mut self, ctx: WindowContext) -> ArconResult<()>;
}

pub struct AppenderWindow<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    buffer: BoxedVecState<IN, u64, u64>,
    materializer: &'static dyn SafelySendableFn(&[IN]) -> OUT,
}

impl<IN, OUT> AppenderWindow<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    pub fn new(
        materializer: &'static dyn SafelySendableFn(&[IN]) -> OUT,
        state_backend: &mut dyn StateBackend,
    ) -> AppenderWindow<IN, OUT> {
        AppenderWindow {
            buffer: state_backend
                .build("appender_window_buffer")
                .with_init_item_key(0)
                .with_init_namespace(0)
                .vec(),
            materializer,
        }
    }
}

impl<IN, OUT> Window<IN, OUT> for AppenderWindow<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    fn on_element(&mut self, element: IN, ctx: WindowContext) -> ArconResult<()> {
        self.buffer.set_current_key(ctx.key)?;
        self.buffer.set_current_namespace(ctx.index)?;

        self.buffer.append(ctx.state_backend, element)?;
        Ok(())
    }

    fn result(&mut self, ctx: WindowContext) -> ArconResult<OUT> {
        self.buffer.set_current_key(ctx.key)?;
        self.buffer.set_current_namespace(ctx.index)?;

        let buf = self.buffer.get(ctx.state_backend)?;
        Ok((self.materializer)(&buf))
    }

    fn clear(&mut self, ctx: WindowContext) -> ArconResult<()> {
        self.buffer.set_current_key(ctx.key)?;
        self.buffer.set_current_namespace(ctx.index)?;

        self.buffer.clear(ctx.state_backend)?;
        Ok(())
    }
}

pub struct IncrementalWindow<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    aggregator: BoxedAggregatingState<IN, OUT, u64, u64>,
}

impl<IN, OUT> IncrementalWindow<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    pub fn new(
        init: &'static dyn SafelySendableFn(IN) -> OUT,
        agg: &'static dyn SafelySendableFn(IN, &OUT) -> OUT,
        state_backend: &mut dyn StateBackend,
    ) -> IncrementalWindow<IN, OUT> {
        #[derive(Clone)]
        struct IncrementalWindowAggregator<IN: ArconType, OUT: ArconType>(
            &'static dyn SafelySendableFn(IN) -> OUT,
            &'static dyn SafelySendableFn(IN, &OUT) -> OUT,
        );

        impl<IN: ArconType, OUT: ArconType> Aggregator<IN> for IncrementalWindowAggregator<IN, OUT> {
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

        let aggregator = state_backend
            .build("incremental_window_aggregating_state")
            .with_init_item_key(0)
            .with_init_namespace(0)
            .aggregating(IncrementalWindowAggregator(init, agg));

        IncrementalWindow { aggregator }
    }
}

impl<IN, OUT> Window<IN, OUT> for IncrementalWindow<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    fn on_element(&mut self, element: IN, ctx: WindowContext) -> ArconResult<()> {
        self.aggregator.set_current_key(ctx.key)?;
        self.aggregator.set_current_namespace(ctx.index)?;

        self.aggregator.append(ctx.state_backend, element)?;

        Ok(())
    }

    fn result(&mut self, ctx: WindowContext) -> ArconResult<OUT> {
        self.aggregator.set_current_key(ctx.key)?;
        self.aggregator.set_current_namespace(ctx.index)?;

        self.aggregator.get(ctx.state_backend)
    }

    fn clear(&mut self, ctx: WindowContext) -> ArconResult<()> {
        self.aggregator.set_current_key(ctx.key)?;
        self.aggregator.set_current_namespace(ctx.index)?;

        self.aggregator.clear(ctx.state_backend)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sum_appender_window_test() {
        let mut state_backend = InMemory::new("test").unwrap();

        fn materializer(buffer: &[i32]) -> i32 {
            buffer.iter().sum()
        }
        let mut window: AppenderWindow<i32, i32> =
            AppenderWindow::new(&materializer, &mut state_backend);
        for i in 0..10 {
            let _ = window.on_element(i, WindowContext::new(&mut state_backend, 0, 0));
        }

        let sum = window
            .result(WindowContext::new(&mut state_backend, 0, 0))
            .unwrap();
        let expected: i32 = 45;
        assert_eq!(sum, expected);
    }

    #[test]
    fn sum_incremental_window_test() {
        let mut state_backend = InMemory::new("test").unwrap();

        fn init(i: i32) -> u64 {
            i as u64
        }

        fn aggregation(i: i32, agg: &u64) -> u64 {
            agg + i as u64
        }

        let mut window: IncrementalWindow<i32, u64> =
            IncrementalWindow::new(&init, &aggregation, &mut state_backend);

        for i in 0..10 {
            let _ = window.on_element(i, WindowContext::new(&mut state_backend, 0, 0));
        }

        let sum = window
            .result(WindowContext::new(&mut state_backend, 0, 0))
            .unwrap();
        let expected: u64 = 45;
        assert_eq!(sum, expected);
    }
}
