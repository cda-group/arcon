// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

/// Available function operators
pub mod function;
/// Available sink operators
pub mod sink;
/// Available window operators
pub mod window;

use crate::{
    data::{ArconElement, ArconEvent, ArconType, Epoch, Watermark},
    prelude::state,
    stream::channel::strategy::ChannelStrategy,
    timer::TimerBackend,
};
use kompact::prelude::ComponentDefinition;
use prost::Message;

/// Defines the methods an `Operator` must implement
pub trait Operator<B: state::Backend>: Send + Sized {
    /// The type of input elements this operator processes
    type IN: ArconType;
    /// The type of output elements this operator produces
    type OUT: ArconType;
    /// Storage state type for timer facilities
    type TimerState: Message + Default + PartialEq;

    /// Called by the parent Node to register states used by this Operator
    fn register_states(&mut self, registration_token: &mut state::RegistrationToken<B>);

    fn init(&mut self, session: &mut state::Session<B>);

    /// Determines how the `Operator` processes Elements
    fn handle_element<CD>(
        &self,
        element: ArconElement<Self::IN>,
        source: &CD,
        ctx: OperatorContext<Self, B, impl TimerBackend<Self::TimerState>>,
    ) where
        CD: ComponentDefinition + Sized + 'static;

    /// Determines how the `Operator` processes Watermarks
    fn handle_watermark<CD>(
        &self,
        watermark: Watermark,
        source: &CD,
        ctx: OperatorContext<Self, B, impl TimerBackend<Self::TimerState>>,
    ) where
        CD: ComponentDefinition + Sized + 'static;

    /// Determines how the `Operator` processes an Epoch marker
    fn handle_epoch<CD>(
        &self,
        epoch: Epoch,
        source: &CD,
        ctx: OperatorContext<Self, B, impl TimerBackend<Self::TimerState>>,
    ) where
        CD: ComponentDefinition + Sized + 'static;

    /// Determines how the `Operator` handles timeouts it registered earlier when they are triggered
    fn handle_timeout<CD>(
        &self,
        timeout: Self::TimerState,
        source: &CD,
        ctx: OperatorContext<Self, B, impl TimerBackend<Self::TimerState>>,
    ) where
        CD: ComponentDefinition + Sized + 'static;
}

/// Helper macro to implement an empty ´handle_watermark` function
#[macro_export]
macro_rules! ignore_watermark {
    ($backend:ty) => {
        fn handle_watermark<CD>(
            &self,
            _watermark: Watermark,
            _source: &CD,
            _ctx: OperatorContext<Self, $backend, impl TimerBackend<Self::TimerState>>,
        ) where
            CD: ComponentDefinition + Sized + 'static,
        {
        }
    };
}

/// Helper macro to implement an empty ´handle_epoch` function
#[macro_export]
macro_rules! ignore_epoch {
    ($backend:ty) => {
        fn handle_epoch<CD>(
            &self,
            _epoch: Epoch,
            _source: &CD,
            _ctx: OperatorContext<Self, $backend, impl TimerBackend<Self::TimerState>>,
        ) where
            CD: ComponentDefinition + Sized + 'static,
        {
        }
    };
}

/// Helper macro to implement an empty ´handle_timeout` function
#[macro_export]
macro_rules! ignore_timeout {
    ($backend:ty) => {
        fn handle_timeout<CD>(
            &self,
            _timeout: Self::TimerState,
            _source: &CD,
            _ctx: OperatorContext<Self, $backend, impl TimerBackend<Self::TimerState>>,
        ) where
            CD: ComponentDefinition + Sized + 'static,
        {
        }
    };
}

pub struct OperatorContext<'c, 's, 'b, 't, OP, B, T>
where
    OP: Operator<B>,
    B: state::Backend,
    T: TimerBackend<OP::TimerState>,
{
    channel_strategy: &'c mut ChannelStrategy<OP::OUT>,
    pub state_session: &'s mut state::Session<'b, B>,
    timer_backend: &'t mut T,
}

impl<'c, 's, 'b, 't, OP, B, T> OperatorContext<'c, 's, 'b, 't, OP, B, T>
where
    OP: Operator<B>,
    B: state::Backend,
    T: TimerBackend<OP::TimerState>,
{
    #[inline]
    pub fn new(
        channel_strategy: &'c mut ChannelStrategy<OP::OUT>,
        state_session: &'s mut state::Session<'b, B>,
        timer_backend: &'t mut T,
    ) -> Self {
        OperatorContext {
            channel_strategy,
            state_session,
            timer_backend,
        }
    }

    /// Add an event to the channel strategy
    #[inline]
    pub fn output<CD>(&mut self, event: ArconEvent<OP::OUT>, source: &CD)
    where
        CD: ComponentDefinition + Sized + 'static,
    {
        self.channel_strategy.add(event, source)
    }

    // These are just simpler versions of the TimerBackend API.
    // This way we don't have to manage the passing of the state_backend everywhere.

    #[inline]
    pub fn current_time(&mut self) -> u64 {
        self.timer_backend.current_time(self.state_session)
    }

    #[inline]
    pub fn schedule_after(
        &mut self,
        delay: u64,
        entry: OP::TimerState,
    ) -> Result<(), OP::TimerState> {
        self.timer_backend
            .schedule_after(delay, entry, self.state_session)
    }

    #[inline]
    pub fn schedule_at(&mut self, time: u64, entry: OP::TimerState) -> Result<(), OP::TimerState> {
        self.timer_backend
            .schedule_at(time, entry, self.state_session)
    }
}
