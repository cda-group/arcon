// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

/// Available function operators
pub mod function;
// Available sink operators
//pub mod sink;
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
use std::marker::PhantomData;

/// As we are using Slog with Kompact, we crate an arcon alias for it.
pub type ArconLogger = kompact::KompactLogger;

/// Defines the methods an `Operator` must implement
pub trait Operator<B: Backend>: Send + Sized {
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
        ctx: OperatorContext<Self, B, impl ComponentDefinition>,
    ) -> ArconResult<()>;

    /// Determines how the `Operator` handles timeouts it registered earlier when they are triggered
    fn handle_timeout(
        &self,
        timeout: Self::TimerState,
        ctx: OperatorContext<Self, B, impl ComponentDefinition>,
    ) -> ArconResult<()>;

    /// Determines how the `Operator` persists its state
    fn persist(&mut self) -> Result<(), arcon_state::error::ArconStateError>;
}

/// Helper macro to implement an empty ´handle_timeout` function
#[macro_export]
macro_rules! ignore_timeout {
    ($backend:ty) => {
        fn handle_timeout(
            &self,
            _timeout: Self::TimerState,
            _ctx: OperatorContext<Self, $backend, impl ComponentDefinition>,
        ) -> ArconResult<()> {
            Ok(())
        }
    };
}

/// Helper macro to implement an empty ´persist` function
#[macro_export]
macro_rules! ignore_persist {
    () => {
        fn persist(&mut self) -> Result<(), arcon_state::error::ArconStateError> {
            Ok(())
        }
    };
}

/// Context Available to an Arcon Operator
pub struct OperatorContext<'a, 'c, 'b, OP, B, CD>
where
    OP: Operator<B> + 'static,
    B: Backend,
    CD: ComponentDefinition + Sized + 'static,
{
    /// Channel Strategy that is used to pass on events
    channel_strategy: &'c mut ChannelStrategy<OP::OUT>,
    //timer: &'b mut TimerIndex<u64, OP::TimerState, B>,
    /// A reference to the backing ComponentDefinition
    source: &'a CD,
    _marker: &'b PhantomData<B>,
}

impl<'a, 'c, 'b, OP, B, CD> OperatorContext<'a, 'c, 'b, OP, B, CD>
where
    OP: Operator<B> + 'static,
    B: Backend,
    CD: ComponentDefinition + Sized + 'static,
{
    #[inline]
    pub fn new(
        source: &'a CD,
        //timer: &'b mut TimerIndex<u64, OP::TimerState, B>,
        channel_strategy: &'c mut ChannelStrategy<OP::OUT>,
    ) -> Self {
        OperatorContext {
            channel_strategy,
            //timer,
            source,
            _marker: &PhantomData,
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

    #[inline]
    pub fn schedule_at(
        &mut self,
        _time: u64,
        _entry: OP::TimerState,
    ) -> Result<(), arcon_state::error::ArconStateError> {
        // TODO:  Fix TimerIndex and integrate
        //self.timer.schedule_at(time, entry)
        unimplemented!()
    }
}
