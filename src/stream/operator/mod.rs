// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

/// Available function operators
pub mod function;
/// Available sink operators
pub mod sink;
// Available window operators
//pub mod window;

use crate::{
    data::{ArconElement, ArconEvent, ArconType},
    stream::channel::strategy::ChannelStrategy,
};
use arcon_error::*;
use arcon_state::{index::ArconState, Backend, TimerIndex};
use kompact::prelude::ComponentDefinition;
use prost::Message;

/// As we are using Slog with Kompact, we crate an arcon alias for it.
pub type ArconLogger = kompact::KompactLogger;

/// Defines the methods an `Operator` must implement
pub trait Operator: Send + Sized {
    /// The type of input elements this operator processes
    type IN: ArconType;
    /// The type of output elements this operator produces
    type OUT: ArconType;
    /// Storage state type for timer facilities
    type TimerState: Message + Clone + Default + PartialEq;
    /// State type for the Operator
    type OperatorState: ArconState;

    /// Determines how the `Operator` processes Elements
    fn handle_element(
        &mut self,
        element: ArconElement<Self::IN>,
        ctx: OperatorContext<Self, impl Backend, impl ComponentDefinition>,
    ) -> OperatorResult<()>;

    /// Determines how the `Operator` handles timeouts it registered earlier when they are triggered
    fn handle_timeout(
        &mut self,
        timeout: Self::TimerState,
        ctx: OperatorContext<Self, impl Backend, impl ComponentDefinition>,
    ) -> OperatorResult<()>;

    /// Determines how the `Operator` persists its state
    fn persist(&mut self) -> OperatorResult<()>;
}

/// Helper macro to implement an empty ´handle_timeout` function
#[macro_export]
macro_rules! ignore_timeout {
    () => {
        fn handle_timeout(
            &mut self,
            _timeout: Self::TimerState,
            _ctx: OperatorContext<Self, impl Backend, impl ComponentDefinition>,
        ) -> OperatorResult<()> {
            Ok(())
        }
    };
}

/// Helper macro to implement an empty ´persist` function
#[macro_export]
macro_rules! ignore_persist {
    () => {
        fn persist(&mut self) -> OperatorResult<()> {
            Ok(())
        }
    };
}

/// Context Available to an Arcon Operator
pub struct OperatorContext<'a, 'c, 'b, OP, B, CD>
where
    OP: Operator + 'static,
    B: Backend,
    CD: ComponentDefinition + Sized + 'static,
{
    /// Channel Strategy that is used to pass on events
    channel_strategy: &'c mut ChannelStrategy<OP::OUT>,
    /// A TimerIndex that can be used to schedule event timers
    timer: &'b mut Option<TimerIndex<u64, OP::TimerState, B>>,
    /// A reference to the backing ComponentDefinition
    source: &'a CD,
}

impl<'a, 'c, 'b, OP, B, CD> OperatorContext<'a, 'c, 'b, OP, B, CD>
where
    OP: Operator + 'static,
    B: Backend,
    CD: ComponentDefinition + Sized + 'static,
{
    #[inline]
    pub(crate) fn new(
        source: &'a CD,
        timer: &'b mut Option<TimerIndex<u64, OP::TimerState, B>>,
        channel_strategy: &'c mut ChannelStrategy<OP::OUT>,
    ) -> Self {
        OperatorContext {
            channel_strategy,
            timer,
            source,
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
        self.source.log()
    }

    /// Get current event time
    #[inline]
    pub fn current_time(&mut self) -> u64 {
        if let Some(timer) = self.timer {
            timer.current_time()
        } else {
            panic!("Can not fetch current time with uninitialised timer");
        }
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
    ) -> Result<(), OP::TimerState> {
        if let Some(timer) = self.timer {
            timer.schedule_at(key.into(), time, entry)
        } else {
            panic!("Can not schedule timers with an uninitialised timer");
        }
    }
}
