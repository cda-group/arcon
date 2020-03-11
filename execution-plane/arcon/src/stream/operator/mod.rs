// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

pub mod function;
pub mod sink;
pub mod window;

use crate::{
    data::{ArconElement, ArconEvent, ArconType, Epoch, Watermark},
    state_backend::StateBackend,
    stream::channel::strategy::ChannelStrategy,
};
use arcon_error::ArconResult;

/// Defines the methods an `Operator` must implement
pub trait Operator<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    /// Called by parent node to finish the initialization of the operator
    fn init(&mut self, _state_backend: &mut dyn StateBackend) {}

    /// Determines how the `Operator` processes Elements
    ///
    /// The function takes an Element and a [NodeContext] in order to pass on it
    fn handle_element(&mut self, element: ArconElement<IN>, ctx: OperatorContext<OUT>);

    /// Determines how the `Operator` processes Watermarks
    ///
    /// The function either returns None or a Vec of ArconEvents (e.g., Window Computation)
    fn handle_watermark(
        &mut self,
        watermark: Watermark,
        ctx: OperatorContext<OUT>,
    ) -> Option<Vec<ArconEvent<OUT>>>;

    /// Determines how the `Operator` processes an Epoch marker
    ///
    /// The function either returns None signaling it did not attempt to snapshot any state.
    /// If the `Operator` snapshotted its state, the raw bytes are packed into an `ArconResult`
    fn handle_epoch(
        &mut self,
        epoch: Epoch,
        ctx: OperatorContext<OUT>,
    ) -> Option<ArconResult<Vec<u8>>>;
}

pub struct OperatorContext<'c, 's, OUT: ArconType> {
    channel_strategy: &'c mut ChannelStrategy<OUT>,
    pub state_backend: &'s mut dyn StateBackend,
}

impl<'c, 's, OUT> OperatorContext<'c, 's, OUT>
where
    OUT: ArconType,
{
    #[inline]
    pub fn new(
        channel_strategy: &'c mut ChannelStrategy<OUT>,
        state_backend: &'s mut dyn StateBackend,
    ) -> OperatorContext<'c, 's, OUT> {
        OperatorContext {
            channel_strategy,
            state_backend,
        }
    }

    #[inline]
    pub fn output(&mut self, event: ArconEvent<OUT>) {
        self.channel_strategy.add(event)
    }
}
