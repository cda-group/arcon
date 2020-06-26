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
    timer::TimerBackend,
};
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
    fn handle_element(
        &self,
        element: ArconElement<Self::IN>,
        ctx: OperatorContext<Self, B, impl TimerBackend<Self::TimerState>>,
    ) -> Vec<ArconEvent<Self::OUT>>;

    /// Determines how the `Operator` processes Watermarks
    fn handle_watermark(
        &self,
        watermark: Watermark,
        ctx: OperatorContext<Self, B, impl TimerBackend<Self::TimerState>>,
    ) -> Option<Vec<ArconEvent<Self::OUT>>>;

    /// Determines how the `Operator` processes an Epoch marker
    fn handle_epoch(
        &self,
        epoch: Epoch,
        ctx: OperatorContext<Self, B, impl TimerBackend<Self::TimerState>>,
    ) -> Option<Vec<ArconEvent<Self::OUT>>>;

    /// Determines how the `Operator` handles timeouts it registered earlier when they are triggered
    fn handle_timeout(
        &self,
        timeout: Self::TimerState,
        ctx: OperatorContext<Self, B, impl TimerBackend<Self::TimerState>>,
    ) -> Option<Vec<ArconEvent<Self::OUT>>>;
}

#[macro_export]
macro_rules! ignore_watermark {
    ($backend:ty) => {
        fn handle_watermark(
            &self,
            _w: Watermark,
            _ctx: OperatorContext<Self, $backend, impl TimerBackend<Self::TimerState>>,
        ) -> Option<Vec<ArconEvent<Self::OUT>>> {
            None
        }
    };
}

#[macro_export]
macro_rules! ignore_epoch {
    ($backend:ty) => {
        fn handle_epoch(
            &self,
            _epoch: Epoch,
            _ctx: OperatorContext<Self, $backend, impl TimerBackend<Self::TimerState>>,
        ) -> Option<Vec<ArconEvent<Self::OUT>>> {
            None
        }
    };
}

#[macro_export]
macro_rules! ignore_timeout {
    ($backend:ty) => {
        fn handle_timeout(
            &self,
            _timeout: Self::TimerState,
            _ctx: OperatorContext<Self, $backend, impl TimerBackend<Self::TimerState>>,
        ) -> Option<Vec<ArconEvent<Self::OUT>>> {
            None
        }
    };
}

pub struct OperatorContext<'s, 'b, 't, OP, B, T>
where
    OP: Operator<B>,
    B: state::Backend,
    T: TimerBackend<OP::TimerState>,
{
    pub state_session: &'s mut state::Session<'b, B>,
    timer_backend: &'t mut T,
    _marker: std::marker::PhantomData<OP>,
}

impl<'s, 'b, 't, OP, B, T> OperatorContext<'s, 'b, 't, OP, B, T>
where
    OP: Operator<B>,
    B: state::Backend,
    T: TimerBackend<OP::TimerState>,
{
    #[inline]
    pub fn new(state_session: &'s mut state::Session<'b, B>, timer_backend: &'t mut T) -> Self {
        OperatorContext {
            state_session,
            timer_backend,
            _marker: std::marker::PhantomData,
        }
    }

    // These are just simpler versions of the TimerBackend API.
    // This way we don't have to manage the passing of the state_backend everywhere.

    pub fn current_time(&mut self) -> u64 {
        self.timer_backend.current_time(self.state_session)
    }

    pub fn schedule_after(
        &mut self,
        delay: u64,
        entry: OP::TimerState,
    ) -> Result<(), OP::TimerState> {
        self.timer_backend
            .schedule_after(delay, entry, self.state_session)
    }

    pub fn schedule_at(&mut self, time: u64, entry: OP::TimerState) -> Result<(), OP::TimerState> {
        self.timer_backend
            .schedule_at(time, entry, self.state_session)
    }
}
