// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

/// Available function operators
pub mod function;
/// Available sink operators
pub mod sink;
/// Available window operators
pub mod window;
#[cfg(feature = "metrics")]
use metrics::{gauge, increment_counter, register_counter, register_gauge};

use crate::{
    application::conf::logger::ArconLogger,
    data::{ArconElement, ArconType},
    error::{timer::TimerResult, *},
    index::{ArconState, Timer},
};
use arcon_state::Backend;
use prost::Message;

/// Defines the methods an `Operator` must implement
pub trait Operator: Send + Sized {
    /// The type of input elements this operator processes
    type IN: ArconType;
    /// The type of output elements this operator produces
    type OUT: ArconType;
    /// Storage state type for timer facilities
    type TimerState: Message + Clone + Default;
    /// State type for the Operator
    type OperatorState: ArconState;

    type ElementIterator: IntoIterator<Item = ArconElement<Self::OUT>> + 'static;

    /// Determines what the `Operator` runs before beginning to process Elements
    fn on_start(&mut self, mut _ctx: OperatorContext<Self, impl Backend>) -> ArconResult<()> {
        Ok(())
    }

    /// Determines how the `Operator` processes Elements
    fn handle_element(
        &mut self,
        element: ArconElement<Self::IN>,
        ctx: OperatorContext<Self, impl Backend>,
    ) -> ArconResult<Self::ElementIterator>;

    /// Determines how the `Operator` handles timeouts it registered earlier when they are triggered
    fn handle_timeout(
        &mut self,
        timeout: Self::TimerState,
        ctx: OperatorContext<Self, impl Backend>,
    ) -> ArconResult<Option<Self::ElementIterator>>;

    /// Determines how the `Operator` persists its state
    fn persist(&mut self) -> ArconResult<()>;

    /// A get function to the operator's state.
    ///
    /// Use the ``ignore_state!()`` macro to indicate its an empty state.
    fn state(&mut self) -> &mut Self::OperatorState;
}

/// Helper macro to implement an empty ´handle_timeout` function
#[macro_export]
macro_rules! ignore_timeout {
    () => {
        fn handle_timeout(
            &mut self,
            _timeout: Self::TimerState,
            _ctx: OperatorContext<Self, impl Backend>,
        ) -> ArconResult<Option<Self::ElementIterator>> {
            Ok(None)
        }
    };
}

/// Helper macro to implement an empty ´persist` function
#[macro_export]
macro_rules! ignore_persist {
    () => {
        fn persist(&mut self) -> ArconResult<()> {
            Ok(())
        }
    };
}

/// Helper macro to implement an empty ´state` function
#[macro_export]
macro_rules! ignore_state {
    () => {
        fn state(&mut self) -> &mut Self::OperatorState {
            crate::index::EmptyState
        }
    };
}

/// Context Available to an Arcon Operator
pub struct OperatorContext<'b, 'd, OP, B>
where
    OP: Operator + 'static,
    B: Backend,
{
    /// A Timer that can be used to schedule event timers
    timer: &'b mut Timer<u64, OP::TimerState, B>,
    /// Reference to logger
    logger: &'d ArconLogger,
    #[cfg(feature = "metrics")]
    name: &'d str,
}

//Note the _ prefixed name field, this is due to the presence of feature flag .Therefore this var might not be used.
impl<'b, 'd, OP, B> OperatorContext<'b, 'd, OP, B>
where
    OP: Operator + 'static,
    B: Backend,
{
    #[inline]
    pub(crate) fn new(
        timer: &'b mut Timer<u64, OP::TimerState, B>,
        logger: &'d ArconLogger,
        #[cfg(feature = "metrics")] _name: &'d str,
    ) -> Self {
        OperatorContext {
            timer,
            logger,

            #[cfg(feature = "metrics")]
            name: _name,
        }
    }

    /// Enable users to log within an Operator
    ///
    /// `error!(ctx.log(), "Something bad happened!");
    #[inline]
    pub fn log(&self) -> &ArconLogger {
        self.logger
    }

    /// Get current event time
    #[inline]
    pub fn current_time(&mut self) -> StateResult<u64> {
        self.timer.current_time()
    }

    /// Schedule at a specific time in the future
    ///
    /// Returns Ok if the entry was scheduled successfully
    /// or `Err(entry)` if it has already expired.
    #[inline]
    pub fn schedule_at<I: Into<u64>>(
        &mut self,
        key: I,
        time: u64,
        entry: OP::TimerState,
        //) -> Result<(), OP::TimerState> {
    ) -> TimerResult<OP::TimerState> {
        self.timer.schedule_at(key.into(), time, entry)
    }

    #[cfg(feature = "metrics")]
    pub fn register_gauge(&mut self, name: &'static str) {
        register_gauge!(format!("{}_{}", self.name, name));
    }

    #[cfg(feature = "metrics")]
    pub fn update_gauge(&self, name: &'static str, value: f64) {
        gauge!(format!("{}_{}", self.name, name), value);
    }

    #[cfg(feature = "metrics")]
    pub fn register_counter(&self, name: &'static str) {
        register_counter!(format!("{}_{}", self.name, name));
    }

    #[cfg(feature = "metrics")]
    pub fn increment_counter(&self, name: &'static str) {
        increment_counter!(format!("{}_{}", self.name, name));
    }
}
