// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

pub mod event_time;

pub use event_time::EventTimeWindowAssigner;

use crate::{
    prelude::*,
    util::{prost_helpers::ProstOption, SafelySendableFn},
};
use arcon_state::{AggregatorState, VecState};

pub struct WindowContext<'s, 'b, B: state::Backend> {
    state_session: &'s mut state::Session<'b, B>,
    key: u64,
    index: u64,
}

impl<'s, 'b, B: state::Backend> WindowContext<'s, 'b, B> {
    pub fn new(state_session: &'s mut state::Session<'b, B>, key: u64, index: u64) -> Self {
        WindowContext {
            state_session,
            key,
            index,
        }
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
    fn register_states(
        &mut self,
        registration_token: &mut state::RegistrationToken<impl state::Backend>,
    );

    /// The `on_element` function is called per received window element
    fn on_element(&self, element: IN, ctx: WindowContext<impl state::Backend>) -> ArconResult<()>;
    /// The `result` function is called at the end of a window's lifetime
    fn result(&self, ctx: WindowContext<impl state::Backend>) -> ArconResult<OUT>;
    /// Clears the window state for the passed context
    fn clear(&self, ctx: WindowContext<impl state::Backend>) -> ArconResult<()>;
}

pub struct AppenderWindow<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    buffer: state::Handle<VecState<IN>, u64, u64>,
    materializer: &'static dyn SafelySendableFn(&[IN]) -> OUT,
}

impl<IN, OUT> AppenderWindow<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    pub fn new(
        materializer: &'static dyn SafelySendableFn(&[IN]) -> OUT,
    ) -> AppenderWindow<IN, OUT> {
        AppenderWindow {
            buffer: state::Handle::vec("appender_window_buffer")
                .with_item_key(0)
                .with_namespace(0),
            materializer,
        }
    }
}

impl<IN, OUT> Window<IN, OUT> for AppenderWindow<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    fn register_states(
        &mut self,
        registration_token: &mut state::RegistrationToken<impl state::Backend>,
    ) {
        self.buffer.register(registration_token)
    }

    fn on_element(&self, element: IN, ctx: WindowContext<impl state::Backend>) -> ArconResult<()> {
        self.buffer.set_item_key(ctx.key);
        self.buffer.set_namespace(ctx.index);

        self.buffer.activate(ctx.state_session).append(element)?;
        Ok(())
    }

    fn result(&self, ctx: WindowContext<impl state::Backend>) -> ArconResult<OUT> {
        self.buffer.set_item_key(ctx.key);
        self.buffer.set_namespace(ctx.index);

        let buf = self.buffer.activate(ctx.state_session).get()?;
        Ok((self.materializer)(&buf))
    }

    fn clear(&self, ctx: WindowContext<impl state::Backend>) -> ArconResult<()> {
        self.buffer.set_item_key(ctx.key);
        self.buffer.set_namespace(ctx.index);

        self.buffer.activate(ctx.state_session).clear()?;
        Ok(())
    }
}

#[derive(Clone)]
struct IncrementalWindowAggregator<IN: ArconType, OUT: ArconType>(
    &'static dyn SafelySendableFn(IN) -> OUT,
    &'static dyn SafelySendableFn(IN, &OUT) -> OUT,
);

impl<IN: ArconType, OUT: ArconType> state::Aggregator for IncrementalWindowAggregator<IN, OUT> {
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

pub struct IncrementalWindow<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    aggregator: state::Handle<AggregatorState<IncrementalWindowAggregator<IN, OUT>>, u64, u64>,
}

impl<IN, OUT> IncrementalWindow<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    pub fn new(
        init: &'static dyn SafelySendableFn(IN) -> OUT,
        agg: &'static dyn SafelySendableFn(IN, &OUT) -> OUT,
    ) -> IncrementalWindow<IN, OUT> {
        let aggregator = state::Handle::aggregator(
            "incremental_window_aggregating_state",
            IncrementalWindowAggregator(init, agg),
        )
        .with_item_key(0)
        .with_namespace(0);

        IncrementalWindow { aggregator }
    }
}

impl<IN, OUT> Window<IN, OUT> for IncrementalWindow<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    fn register_states(
        &mut self,
        registration_token: &mut state::RegistrationToken<impl state::Backend>,
    ) {
        self.aggregator.register(registration_token)
    }

    fn on_element(&self, element: IN, ctx: WindowContext<impl state::Backend>) -> ArconResult<()> {
        self.aggregator.set_item_key(ctx.key);
        self.aggregator.set_namespace(ctx.index);

        self.aggregator
            .activate(ctx.state_session)
            .aggregate(element)?;

        Ok(())
    }

    fn result(&self, ctx: WindowContext<impl state::Backend>) -> ArconResult<OUT> {
        self.aggregator.set_item_key(ctx.key);
        self.aggregator.set_namespace(ctx.index);

        Ok(self.aggregator.activate(ctx.state_session).get()?)
    }

    fn clear(&self, ctx: WindowContext<impl state::Backend>) -> ArconResult<()> {
        self.aggregator.set_item_key(ctx.key);
        self.aggregator.set_namespace(ctx.index);

        Ok(self.aggregator.activate(ctx.state_session).clear()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::InMemory;

    #[test]
    fn sum_appender_window_test() {
        let state_backend = InMemory::create("test".as_ref()).unwrap();
        let mut session = state_backend.session();

        fn materializer(buffer: &[i32]) -> i32 {
            buffer.iter().sum()
        }
        let mut window: AppenderWindow<i32, i32> = AppenderWindow::new(&materializer);
        window.register_states(&mut unsafe { state::RegistrationToken::new(&mut session) });

        for i in 0..10 {
            let _ = window.on_element(i, WindowContext::new(&mut session, 0, 0));
        }

        let sum = window
            .result(WindowContext::new(&mut session, 0, 0))
            .unwrap();
        let expected: i32 = 45;
        assert_eq!(sum, expected);
    }

    #[test]
    fn sum_incremental_window_test() {
        let state_backend = InMemory::create("test".as_ref()).unwrap();
        let mut session = state_backend.session();

        fn init(i: i32) -> u64 {
            i as u64
        }
        fn aggregation(i: i32, agg: &u64) -> u64 {
            agg + i as u64
        }

        let mut window: IncrementalWindow<i32, u64> = IncrementalWindow::new(&init, &aggregation);
        window.register_states(&mut unsafe { state::RegistrationToken::new(&mut session) });

        for i in 0..10 {
            let _ = window.on_element(i, WindowContext::new(&mut session, 0, 0));
        }

        let sum = window
            .result(WindowContext::new(&mut session, 0, 0))
            .unwrap();
        let expected: u64 = 45;
        assert_eq!(sum, expected);
    }
}
