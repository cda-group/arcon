// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::data::{ArconElement, ArconEvent, ArconType, Epoch, Watermark};
use crate::stream::operator::Operator;
use crate::util::SafelySendableFn;
use arcon_error::ArconResult;

/// IN: Input Event
/// OUT: Output Event
pub struct FlatMap<IN, OUT>
where
    IN: 'static + ArconType,
    OUT: 'static + ArconType,
{
    udf: &'static dyn for<'r> SafelySendableFn<(&'r IN,), Vec<OUT>>,
}

impl<IN, OUT> FlatMap<IN, OUT>
where
    IN: 'static + ArconType,
    OUT: 'static + ArconType,
{
    pub fn new(udf: &'static dyn for<'r> SafelySendableFn<(&'r IN,), Vec<OUT>>) -> Self {
        FlatMap { udf }
    }

    #[inline]
    pub fn run_udf(&self, event: &IN) -> Vec<OUT> {
        (self.udf)(event)
    }
}

impl<IN, OUT> Operator<IN, OUT> for FlatMap<IN, OUT>
where
    IN: 'static + ArconType,
    OUT: 'static + ArconType,
{
    fn handle_element(&mut self, element: ArconElement<IN>) -> ArconResult<Vec<ArconEvent<OUT>>> {
        let result = self.run_udf(&(element.data));
        let mut elements = Vec::new();
        for item in result {
            elements.push(ArconEvent::Element(ArconElement {
                data: item,
                timestamp: element.timestamp,
            }));
        }

        Ok(elements)
    }

    fn handle_watermark(&mut self, _w: Watermark) -> ArconResult<Vec<ArconEvent<OUT>>> {
        Ok(Vec::new())
    }
    fn handle_epoch(&mut self, _epoch: Epoch) -> ArconResult<Vec<u8>> {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flatmap_test() {
        fn flatmap_fn(x: &i32) -> Vec<i32> {
            (0..*x).map(|x| x + 5).collect()
        }

        let mut flatmap = FlatMap::new(&flatmap_fn);

        let input = ArconElement::new(6 as i32);
        let result = flatmap.handle_element(input).unwrap();
        let expected: Vec<i32> = vec![5, 6, 7, 8, 9, 10];

        let mut result_vec = Vec::new();
        for event in result {
            if let ArconEvent::Element(element) = event {
                result_vec.push(element.data)
            }
        }

        assert_eq!(result_vec, expected);
    }
}
