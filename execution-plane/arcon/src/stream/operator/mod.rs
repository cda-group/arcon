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
    state_backend::StateBackend,
    stream::channel::strategy::ChannelStrategy,
    timer::TimerBackend,
};
use arcon_error::ArconResult;
use prost::Message;

/// Defines the methods an `Operator` must implement
pub trait Operator: Send + Sized {
    /// The type of input elements this operator processes
    type IN: ArconType;
    /// The type of output elements this operator produces
    type OUT: ArconType;
    /// Storage state type for timer facilities
    type TimerState: Message + Default + PartialEq;

    /// Called by parent node to finish the initialization of the operator
    fn init(&mut self, _state_backend: &mut dyn StateBackend) {}

    /// Determines how the `Operator` processes Elements
    ///
    /// The function takes an Element and a [NodeContext] in order to pass on it
    fn handle_element(&mut self, element: ArconElement<Self::IN>, ctx: OperatorContext<Self>);

    /// Determines how the `Operator` processes Watermarks
    fn handle_watermark(&mut self, watermark: Watermark, ctx: OperatorContext<Self>);

    /// Determines how the `Operator` processes an Epoch marker
    ///
    /// The function either returns None signaling it did not attempt to snapshot any state.
    /// If the `Operator` snapshotted its state, the raw bytes are packed into an `ArconResult`
    fn handle_epoch(
        &mut self,
        epoch: Epoch,
        ctx: OperatorContext<Self>,
    ) -> Option<ArconResult<Vec<u8>>>;

    /// Determines how the `Operator` handles timeouts it registered earlier when they are triggered
    fn handle_timeout(&mut self, timeout: Self::TimerState, ctx: OperatorContext<Self>);
}

pub struct OperatorContext<'c, 's, 't, OP: Operator> {
    channel_strategy: &'c mut ChannelStrategy<OP::OUT>,
    pub state_backend: &'s mut dyn StateBackend,
    pub timer_backend: &'t mut dyn TimerBackend<OP::TimerState>,
}

impl<'c, 's, 't, OP> OperatorContext<'c, 's, 't, OP>
where
    OP: Operator,
{
    #[inline]
    pub fn new(
        channel_strategy: &'c mut ChannelStrategy<OP::OUT>,
        state_backend: &'s mut dyn StateBackend,
        timer_backend: &'t mut dyn TimerBackend<OP::TimerState>,
    ) -> Self {
        OperatorContext {
            channel_strategy,
            state_backend,
            timer_backend,
        }
    }

    #[inline]
    pub fn output(&mut self, event: ArconEvent<OP::OUT>) {
        self.channel_strategy.add(event)
    }
}
