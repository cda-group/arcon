// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

/// Available function operators
pub mod function;
/// Available sink operators
pub mod sink;
/// Available window operators
pub mod window;

use crate::{
    conf::logger::ArconLogger,
    data::{ArconElement, ArconEvent, ArconType},
    error::{timer::TimerResult, *},
    index::{ArconState, Timer},
    stream::channel::strategy::ChannelStrategy,
};
use arcon_state::Backend;
use kompact::prelude::ComponentDefinition;
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

    /// Determines what the `Operator` runs before beginning to process Elements
    fn on_start(
        &mut self,
        mut _ctx: OperatorContext<Self, impl Backend, impl ComponentDefinition>,
    ) -> ArconResult<()> {
        Ok(())
    }

    /// Determines how the `Operator` processes Elements
    fn handle_element(
        &mut self,
        element: ArconElement<Self::IN>,
        ctx: OperatorContext<Self, impl Backend, impl ComponentDefinition>,
    ) -> ArconResult<()>;

    /// Determines how the `Operator` handles timeouts it registered earlier when they are triggered
    fn handle_timeout(
        &mut self,
        timeout: Self::TimerState,
        ctx: OperatorContext<Self, impl Backend, impl ComponentDefinition>,
    ) -> ArconResult<()>;

    /// Determines how the `Operator` persists its state
    fn persist(&mut self) -> StateResult<()>;

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
            _ctx: OperatorContext<Self, impl Backend, impl ComponentDefinition>,
        ) -> ArconResult<()> {
            Ok(())
        }
    };
}

/// Helper macro to implement an empty ´persist` function
#[macro_export]
macro_rules! ignore_persist {
    () => {
        fn persist(&mut self) -> StateResult<()> {
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
pub struct OperatorContext<'a, 'c, 'b, 'd, OP, B, CD>
where
    OP: Operator + 'static,
    B: Backend,
    CD: ComponentDefinition + Sized + 'static,
{
    /// Channel Strategy that is used to pass on events
    channel_strategy: &'c mut ChannelStrategy<OP::OUT>,
    /// A Timer that can be used to schedule event timers
    timer: &'b mut Timer<u64, OP::TimerState, B>,
    /// A reference to the backing ComponentDefinition
    source: &'a CD,
    /// Reference to logger
    logger: &'d ArconLogger,
}

impl<'a, 'c, 'b, 'd, OP, B, CD> OperatorContext<'a, 'c, 'b, 'd, OP, B, CD>
where
    OP: Operator + 'static,
    B: Backend,
    CD: ComponentDefinition + Sized + 'static,
{
    #[inline]
    pub(crate) fn new(
        source: &'a CD,
        timer: &'b mut Timer<u64, OP::TimerState, B>,
        channel_strategy: &'c mut ChannelStrategy<OP::OUT>,
        logger: &'d ArconLogger,
    ) -> Self {
        OperatorContext {
            channel_strategy,
            timer,
            source,
            logger,
        }
    }

    /// Add an event to the channel strategy
    #[inline]
    pub fn output(&mut self, element: ArconElement<OP::OUT>) {
        self.channel_strategy
            .add(ArconEvent::Element(element), self.source)
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
}
