// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::data::{ArconElement, ArconEvent, ArconType, Epoch, Watermark};
use crate::stream::operator::Operator;
use crate::util::SafelySendableFn;
use arcon_error::ArconResult;

/// IN: Input Event
/// OUT: Output Event
pub struct Map<IN, OUT>
where
    IN: 'static + ArconType,
    OUT: 'static + ArconType,
{
    udf: &'static dyn SafelySendableFn<(IN,), OUT>,
}

impl<IN, OUT> Map<IN, OUT>
where
    IN: 'static + ArconType,
    OUT: 'static + ArconType,
{
    pub fn new(udf: &'static dyn SafelySendableFn<(IN,), OUT>) -> Self {
        Map { udf }
    }

    #[inline]
    pub fn run_udf(&self, event: IN) -> OUT {
        (self.udf)(event)
    }
}

impl<IN, OUT> Operator<IN, OUT> for Map<IN, OUT>
where
    IN: 'static + ArconType,
    OUT: 'static + ArconType,
{
    fn handle_element(&mut self, element: ArconElement<IN>) -> Option<Vec<ArconEvent<OUT>>> {
        if let Some(data) = element.data {
            let result = self.run_udf(data);
            Some(vec![ArconEvent::Element(ArconElement {
                data: Some(result),
                timestamp: element.timestamp,
            })])
        } else {
            None
        }
    }

    fn handle_watermark(&mut self, _w: Watermark) -> Option<Vec<ArconEvent<OUT>>> {
        None
    }
    fn handle_epoch(&mut self, _epoch: Epoch) -> Option<ArconResult<Vec<u8>>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_test() {
        fn map_fn(x: i32) -> i32 {
            x + 10
        }

        let mut map = Map::new(&map_fn);

        let input_one = ArconElement::new(6 as i32);
        let input_two = ArconElement::new(7 as i32);
        let r1 = map.handle_element(input_one);
        let r2 = map.handle_element(input_two);
        let mut result_vec = Vec::new();

        result_vec.push(r1);
        result_vec.push(r2);

        let expected: Vec<Option<i32>> = vec![Some(16), Some(17)];
        let mut results = Vec::new();
        for r in result_vec {
            if let Some(result) = r {
                for event in result {
                    if let ArconEvent::Element(element) = event {
                        results.push(element.data)
                    }
                }
            }
        }
        assert_eq!(results, expected);
    }
}
