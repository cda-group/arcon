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
    index::{timer::ArconTimer, ArconState},
};
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
    /// Iterator that produces outgoing elements
    type ElementIterator: IntoIterator<Item = ArconElement<Self::OUT>> + 'static;

    /// Determines what the `Operator` runs before beginning to process Elements
    fn on_start(
        &mut self,
        _ctx: &mut OperatorContext<Self::TimerState, Self::OperatorState>,
    ) -> ArconResult<()> {
        Ok(())
    }

    /// Determines how the `Operator` processes Elements
    fn handle_element(
        &mut self,
        element: ArconElement<Self::IN>,
        ctx: &mut OperatorContext<Self::TimerState, Self::OperatorState>,
    ) -> ArconResult<Self::ElementIterator>;

    /// Determines how the `Operator` handles timeouts it registered earlier when they are triggered
    fn handle_timeout(
        &mut self,
        timeout: Self::TimerState,
        ctx: &mut OperatorContext<Self::TimerState, Self::OperatorState>,
    ) -> ArconResult<Option<Self::ElementIterator>>;
}

/// Helper macro to implement an empty ´handle_timeout` function
#[macro_export]
macro_rules! ignore_timeout {
    () => {
        fn handle_timeout(
            &mut self,
            _timeout: Self::TimerState,
            _ctx: &mut OperatorContext<Self::TimerState, Self::OperatorState>,
        ) -> ArconResult<Option<Self::ElementIterator>> {
            Ok(None)
        }
    };
}

/// Context Available to an Arcon Operator
pub struct OperatorContext<TimerState, OperatorState>
where
    TimerState: Message + Clone + Default,
    OperatorState: ArconState,
{
    /// A Timer that can be used to schedule event timers
    pub(crate) timer: Box<dyn ArconTimer<Key = u64, Value = TimerState>>,
    /// State of the Operator
    pub(crate) state: OperatorState,
    /// Reference to logger
    pub(crate) logger: ArconLogger,
    #[cfg(feature = "metrics")]
    name: String,
}

impl<TimerState, OperatorState> OperatorContext<TimerState, OperatorState>
where
    TimerState: Message + Clone + Default,
    OperatorState: ArconState,
{
    #[inline]
    pub(crate) fn new(
        timer: Box<dyn ArconTimer<Key = u64, Value = TimerState>>,
        state: OperatorState,
        logger: ArconLogger,
        #[cfg(feature = "metrics")] name: String,
    ) -> Self {
        OperatorContext {
            timer,
            state,
            logger,
            #[cfg(feature = "metrics")]
            name,
        }
    }
    #[inline]
    pub fn state(&mut self) -> &mut OperatorState {
        &mut self.state
    }

    /// Enable users to log within an Operator
    ///
    /// `error!(ctx.log(), "Something bad happened!");
    #[inline]
    pub fn log(&self) -> &ArconLogger {
        &self.logger
    }

    /// Get current event time
    #[inline]
    pub fn current_time(&mut self) -> StateResult<u64> {
        self.timer.get_time()
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
        entry: TimerState,
    ) -> TimerResult<TimerState> {
        self.timer.schedule_at(key.into(), time, entry)
    }

    #[cfg(feature = "metrics")]
    pub fn register_gauge(&mut self, name: &str) {
        register_gauge!(format!("{}_{}", self.name, name));
    }

    #[cfg(feature = "metrics")]
    pub fn update_gauge(&self, name: &str, value: f64) {
        gauge!(format!("{}_{}", self.name, name), value);
    }

    #[cfg(feature = "metrics")]
    pub fn register_counter(&self, name: &str) {
        register_counter!(format!("{}_{}", self.name, name));
    }

    #[cfg(feature = "metrics")]
    pub fn increment_counter(&self, name: &str) {
        increment_counter!(format!("{}_{}", self.name, name));
    }
}
