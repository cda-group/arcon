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
use arcon_state::{index::ArconState, Backend, TimerIndex};
use kompact::prelude::ComponentDefinition;
use prost::Message;
use std::marker::PhantomData;

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
    fn handle_element<CD>(
        &mut self,
        element: ArconElement<Self::IN>,
        ctx: OperatorContext<Self, B, CD>,
    ) where
        CD: ComponentDefinition + Sized + 'static;

    /// Determines how the `Operator` handles timeouts it registered earlier when they are triggered
    fn handle_timeout<CD>(&self, timeout: Self::TimerState, ctx: OperatorContext<Self, B, CD>)
    where
        CD: ComponentDefinition + Sized + 'static;

    /// Determines how the `Operator` persists its state
    fn persist(&mut self) -> Result<(), arcon_state::error::ArconStateError>;
}

// MAP
// TODO: Move

pub struct Map<IN, OUT, F, S, B>
where
    IN: ArconType,
    OUT: ArconType,
    F: SafelySendableFn(IN, &mut S) -> OUT,
    S: ArconState,
    B: Backend,
{
    state: S,
    udf: F,
    _marker: PhantomData<fn(IN) -> OUT>,
    _b: PhantomData<B>,
}

impl<IN, OUT, B> Map<IN, OUT, fn(IN, &mut ()) -> OUT, (), B>
where
    IN: ArconType,
    OUT: ArconType,
    B: Backend,
{
    pub fn new(
        udf: impl SafelySendableFn(IN) -> OUT,
    ) -> Map<IN, OUT, impl SafelySendableFn(IN, &mut ()) -> OUT, (), B> {
        let udf = move |input: IN, _: &mut ()| udf(input);
        Map {
            state: (),
            udf,
            _marker: Default::default(),
            _b: PhantomData,
        }
    }
}

impl<IN, OUT, F, S, B> Map<IN, OUT, F, S, B>
where
    IN: ArconType,
    OUT: ArconType,
    F: SafelySendableFn(IN, &mut S) -> OUT,
    S: ArconState,
    B: Backend,
{
    pub fn stateful(state: S, udf: F) -> Self {
        Map {
            state,
            udf,
            _marker: Default::default(),
            _b: PhantomData,
        }
    }
}

impl<IN, OUT, F, S, B> Operator<B> for Map<IN, OUT, F, S, B>
where
    IN: ArconType,
    OUT: ArconType,
    F: SafelySendableFn(IN, &mut S) -> OUT,
    S: ArconState,
    B: Backend,
{
    type IN = IN;
    type OUT = OUT;
    type TimerState = ArconNever;
    type OperatorState = S;

    fn handle_element<CD>(
        &mut self,
        element: ArconElement<IN>,
        mut ctx: OperatorContext<Self, B, CD>,
    ) where
        CD: ComponentDefinition + Sized + 'static,
    {
        ctx.output(ArconElement {
            data: (self.udf)(element.data, &mut self.state),
            timestamp: element.timestamp,
        });
    }

    crate::ignore_timeout!(B);

    fn persist(&mut self) -> Result<(), arcon_state::error::ArconStateError> {
        self.state.persist()
    }
}

// FILTER
// TODO: Move

pub struct Filter<IN, F, S, B>
where
    IN: ArconType,
    F: SafelySendableFn(&IN, &mut S) -> bool,
    S: ArconState,
    B: Backend,
{
    state: S,
    udf: F,
    _marker: PhantomData<fn(IN) -> bool>,
    _b: PhantomData<B>,
}

impl<IN, B> Filter<IN, fn(&IN, &mut ()) -> bool, (), B>
where
    IN: ArconType,
    B: Backend,
{
    pub fn new(
        udf: impl SafelySendableFn(&IN) -> bool,
    ) -> Filter<IN, impl SafelySendableFn(&IN, &mut ()) -> bool, (), B> {
        let udf = move |input: &IN, _: &mut ()| udf(input);
        Filter {
            state: (),
            udf,
            _marker: Default::default(),
            _b: PhantomData,
        }
    }
}

impl<IN, F, S, B> Filter<IN, F, S, B>
where
    IN: ArconType,
    F: SafelySendableFn(&IN, &mut S) -> bool,
    S: ArconState,
    B: Backend,
{
    pub fn stateful(state: S, udf: F) -> Self {
        Filter {
            state,
            udf,
            _marker: Default::default(),
            _b: PhantomData,
        }
    }
}

impl<IN, F, S, B> Operator<B> for Filter<IN, F, S, B>
where
    IN: ArconType,
    F: SafelySendableFn(&IN, &mut S) -> bool,
    S: ArconState,
    B: Backend,
{
    type IN = IN;
    type OUT = IN;
    type TimerState = ArconNever;
    type OperatorState = S;

    fn handle_element<CD>(
        &mut self,
        element: ArconElement<IN>,
        mut ctx: OperatorContext<Self, B, CD>,
    ) where
        CD: ComponentDefinition + Sized + 'static,
    {
        if (self.udf)(&element.data, &mut self.state) {
            ctx.output(element);
        }
    }
    crate::ignore_timeout!(B);

    fn persist(&mut self) -> Result<(), arcon_state::error::ArconStateError> {
        self.state.persist()
    }
}

/// Helper macro to implement an empty ´handle_timeout` function
#[macro_export]
macro_rules! ignore_timeout {
    ($backend:ty) => {
        fn handle_timeout<CD>(
            &self,
            _timeout: Self::TimerState,
            _ctx: OperatorContext<Self, $backend, CD>,
        ) where
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
