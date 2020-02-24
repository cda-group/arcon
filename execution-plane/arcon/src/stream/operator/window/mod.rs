// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

pub mod event_time;

pub use event_time::EventTimeWindowAssigner;

use crate::{data::*, error::*, util::SafelySendableFn};

/// `Window` consists of the methods required by each window implementation
///
/// IN: Element type sent to the Window
/// OUT: Expected output type of the Window
pub trait Window<IN, OUT>: Send + Sync + WindowClone<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    /// The `on_element` function is called per received window element
    fn on_element(&mut self, element: IN) -> ArconResult<()>;
    /// The `result` function is called at the end of a window's lifetime
    fn result(&mut self) -> ArconResult<OUT>;
}

pub trait WindowClone<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    fn clone_box(&self) -> Box<dyn Window<IN, OUT>>;
}

impl<IN, OUT, A: 'static + Window<IN, OUT> + Clone> WindowClone<IN, OUT> for A
where
    IN: ArconType,
    OUT: ArconType,
{
    fn clone_box(&self) -> Box<dyn Window<IN, OUT>> {
        Box::new(self.clone())
    }
}

impl<IN, OUT> Clone for Box<dyn Window<IN, OUT>>
where
    IN: ArconType,
    OUT: ArconType,
{
    fn clone(&self) -> Box<dyn Window<IN, OUT>> {
        self.clone_box()
    }
}

#[derive(Clone)]
pub struct AppenderWindow<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    buffer: Vec<IN>,
    materializer: &'static dyn for<'r> SafelySendableFn<(&'r [IN],), OUT>,
}

impl<IN, OUT> AppenderWindow<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    pub fn new(
        materializer: &'static dyn for<'r> SafelySendableFn<(&'r [IN],), OUT>,
    ) -> AppenderWindow<IN, OUT> {
        AppenderWindow {
            buffer: Vec::new(),
            materializer,
        }
    }
}

impl<IN, OUT> Window<IN, OUT> for AppenderWindow<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    fn on_element(&mut self, element: IN) -> ArconResult<()> {
        self.buffer.push(element);
        Ok(())
    }

    fn result(&mut self) -> ArconResult<OUT> {
        Ok((self.materializer)(&self.buffer))
    }
}

#[derive(Clone)]
pub struct IncrementalWindow<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    curr_agg: Option<OUT>,
    init: &'static dyn SafelySendableFn<(IN,), OUT>,
    agg: &'static dyn for<'r> SafelySendableFn<(IN, &'r OUT), OUT>,
}

impl<IN, OUT> IncrementalWindow<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    pub fn new(
        init: &'static dyn SafelySendableFn<(IN,), OUT>,
        agg: &'static dyn for<'r> SafelySendableFn<(IN, &'r OUT), OUT>,
    ) -> IncrementalWindow<IN, OUT> {
        IncrementalWindow {
            curr_agg: None,
            init,
            agg,
        }
    }
}

impl<IN, OUT> Window<IN, OUT> for IncrementalWindow<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    fn on_element(&mut self, element: IN) -> ArconResult<()> {
        if let Some(ref curr_agg) = self.curr_agg {
            let new_agg = (self.agg)(element, curr_agg);
            self.curr_agg = Some(new_agg);
        } else {
            let init_agg = (self.init)(element);
            self.curr_agg = Some(init_agg);
        }
        Ok(())
    }

    fn result(&mut self) -> ArconResult<OUT> {
        if let Some(curr_agg) = self.curr_agg.take() {
            Ok(curr_agg)
        } else {
            arcon_err!("Something went wrong while fetching window result")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sum_appender_window_test() {
        fn materializer(buffer: &[i32]) -> i32 {
            buffer.iter().sum()
        }
        let mut window: AppenderWindow<i32, i32> = AppenderWindow::new(&materializer);
        for i in 0..10 {
            let _ = window.on_element(i as i32);
        }

        let sum = window.result().unwrap();
        let expected: i32 = 45;
        assert_eq!(sum, expected);
    }

    #[test]
    fn sum_incremental_window_test() {
        fn init(i: i32) -> u64 {
            i as u64
        }

        fn aggregation(i: i32, agg: &u64) -> u64 {
            agg + i as u64
        }

        let mut window: IncrementalWindow<i32, u64> = IncrementalWindow::new(&init, &aggregation);

        for i in 0..10 {
            let _ = window.on_element(i as i32);
        }

        let sum = window.result().unwrap();
        let expected: u64 = 45;
        assert_eq!(sum, expected);
    }
}
