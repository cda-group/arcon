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
    fn handle_element(&mut self, element: ArconElement<IN>) -> Option<Vec<ArconEvent<OUT>>>;
    fn handle_watermark(&mut self, watermark: Watermark) -> Option<Vec<ArconEvent<OUT>>>;
    fn handle_epoch(&mut self, epoch: Epoch) -> ArconResult<Vec<u8>>;
}
