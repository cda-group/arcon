// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

pub mod function;
pub mod sink;
pub mod window;

use crate::data::{ArconElement, ArconEvent, ArconType, Epoch, Watermark};
use arcon_error::ArconResult;

/// Defines the methods an `Operator` must implement
pub trait Operator<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    /// Determines how the `Operator` processes Elements
    ///
    /// The function either returns None or a Vec of ArconEvents.
    /// In order to support FlatMap, the function returns a Vec
    /// rather than a single ArconEvent
    fn handle_element(&mut self, element: ArconElement<IN>) -> Option<Vec<ArconEvent<OUT>>>;
    /// Determines how the `Operator` processes Watermarks
    ///
    /// The function either returns None or a Vec of ArconEvents (e.g., Window Computation)
    fn handle_watermark(&mut self, watermark: Watermark) -> Option<Vec<ArconEvent<OUT>>>;
    /// Determines how the `Operator` processes an Epoch marker
    ///
    /// The function either returns None signaling it did not attempt to snapshot any state.
    /// If the `Operator` snapshotted its state, the raw bytes are packed into an `ArconResult`
    fn handle_epoch(&mut self, epoch: Epoch) -> Option<ArconResult<Vec<u8>>>;
}