// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconElement, ArconEvent, ArconNever, ArconType, Epoch, Watermark},
    stream::operator::{Operator, OperatorContext},
    util::SafelySendableFn,
};
use arcon_error::ArconResult;

/// An Arcon operator for performing an in-place map
///
/// IN: Input Event
pub struct MapInPlace<IN>
where
    IN: ArconType,
{
    udf: &'static dyn SafelySendableFn(&mut IN) -> (),
}

impl<IN> MapInPlace<IN>
where
    IN: ArconType,
{
    pub fn new(udf: &'static dyn SafelySendableFn(&mut IN) -> ()) -> Self {
        MapInPlace { udf }
    }

    #[inline]
    pub fn run_udf(&self, event: &mut IN) {
        (self.udf)(event)
    }
}

impl<IN> Operator for MapInPlace<IN>
where
    IN: ArconType,
{
    type IN = IN;
    type OUT = IN;
    type TimerState = ArconNever;

    fn handle_element(&mut self, element: ArconElement<IN>, mut ctx: OperatorContext<Self>) {
        let mut elem = element;
        if let Some(data) = elem.data.as_mut() {
            self.run_udf(data);
            ctx.output(ArconEvent::Element(elem));
        }
    }

    fn handle_watermark(&mut self, _w: Watermark, _ctx: OperatorContext<Self>) {}
    fn handle_epoch(
        &mut self,
        _epoch: Epoch,
        _ctx: OperatorContext<Self>,
    ) -> Option<ArconResult<Vec<u8>>> {
        None
    }
    fn handle_timeout(&mut self, _timeout: Self::TimerState, _ctx: OperatorContext<Self>) {}
}
