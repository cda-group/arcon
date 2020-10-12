// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

// Available function operators
//pub mod function;
// Available sink operators
//pub mod sink;
// Available window operators
//pub mod window;

use crate::{
    data::{ArconElement, ArconEvent, ArconNever, ArconType},
    stream::channel::strategy::ChannelStrategy,
    util::SafelySendableFn,
};
use arcon_state::index::ArconState;
use kompact::prelude::ComponentDefinition;
use prost::Message;
use std::marker::PhantomData;

/// Defines the methods an `Operator` must implement
pub trait Operator: Send + Sized {
    /// The type of input elements this operator processes
    type IN: ArconType;
    /// The type of output elements this operator produces
    type OUT: ArconType;
    /// Storage state type for timer facilities
    type TimerState: Message + Default + PartialEq;
    /// State type for the Operator
    type OperatorState: ArconState;

    /// Determines how the `Operator` processes Elements
    fn handle_element<CD>(
        &mut self,
        element: ArconElement<Self::IN>,
        ctx: OperatorContext<Self, CD>,
    ) where
        CD: ComponentDefinition + Sized + 'static;

    /// Determines how the `Operator` handles timeouts it registered earlier when they are triggered
    fn handle_timeout<CD>(&self, timeout: Self::TimerState, ctx: OperatorContext<Self, CD>)
    where
        CD: ComponentDefinition + Sized + 'static;

    /// Determines how the `Operator` persists its state
    fn persist(&mut self) -> Result<(), arcon_state::error::ArconStateError>;
}

pub struct Map<IN, OUT, F, S>
where
    IN: ArconType,
    OUT: ArconType,
    F: SafelySendableFn(IN, &mut S) -> OUT,
    S: ArconState,
{
    state: S,
    udf: F,
    _marker: PhantomData<fn(IN) -> OUT>,
}


impl<IN, OUT, F, S> Map<IN, OUT, F, S>
where
    IN: ArconType,
    OUT: ArconType,
    F: SafelySendableFn(IN, &mut S) -> OUT,
    S: ArconState,
{
    pub fn stateful(state: S, udf: F) -> Self {
        Map {
            state,
            udf,
            _marker: Default::default(),
        }
    }
}

impl<IN, OUT, F, S> Operator for Map<IN, OUT, F, S>
where
    IN: ArconType,
    OUT: ArconType,
    F: SafelySendableFn(IN, &mut S) -> OUT,
    S: ArconState,
{
    type IN = IN;
    type OUT = OUT;
    type TimerState = ArconNever;
    type OperatorState = S;

    fn handle_element<CD>(&mut self, element: ArconElement<IN>, mut ctx: OperatorContext<Self, CD>)
    where
        CD: ComponentDefinition + Sized + 'static,
    {
        let result = (self.udf)(element.data, &mut self.state);
        let out_elem = ArconElement {
            data: result,
            timestamp: element.timestamp,
        };
        ctx.output(ArconEvent::Element(out_elem));
    }
    crate::ignore_timeout!();

    fn persist(&mut self) -> Result<(), arcon_state::error::ArconStateError> {
        self.state.persist()
    }
}

/// Helper macro to implement an empty ´handle_timeout` function
#[macro_export]
macro_rules! ignore_timeout {
    () => {
        fn handle_timeout<CD>(&self, _timeout: Self::TimerState, _ctx: OperatorContext<Self, CD>)
        where
            CD: ComponentDefinition + Sized + 'static,
        {
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
pub struct OperatorContext<'a, 'c, OP, CD>
where
    OP: Operator,
    CD: ComponentDefinition + Sized + 'static,
{
    /// Channel Strategy that is used to pass on events
    channel_strategy: &'c mut ChannelStrategy<OP::OUT>,
    /// A reference to the backing ComponentDefinition
    source: &'a CD,
}

impl<'a, 'c, OP, CD> OperatorContext<'a, 'c, OP, CD>
where
    OP: Operator,
    CD: ComponentDefinition + Sized + 'static,
{
    #[inline]
    pub fn new(source: &'a CD, channel_strategy: &'c mut ChannelStrategy<OP::OUT>) -> Self {
        OperatorContext {
            channel_strategy,
            source,
        }
    }

    /// Add an event to the channel strategy
    #[inline]
    pub fn output(&mut self, event: ArconEvent<OP::OUT>) {
        self.channel_strategy.add(event, self.source)
    }

    #[inline]
    pub fn schedule_at(
        &mut self,
        time: u64,
        entry: OP::TimerState,
    ) -> Result<(), arcon_state::error::ArconStateError> {
        // TODO:  Fix TimerIndex and integrate
        //self.timer_backend
        //   .schedule_at(time, entry, self.state_session)
        unimplemented!()
    }
    // TODO: ctx.spawn_async..
}
