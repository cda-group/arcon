// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::data::{ArconElement, ArconEvent, ArconType, Epoch, Watermark};
use crate::stream::operator::Operator;
use crate::util::SafelySendableFn;
use arcon_error::ArconResult;

/// IN: Input Event
pub struct Filter<IN>
where
    IN: 'static + ArconType,
{
    udf: &'static dyn for<'r> SafelySendableFn<(&'r IN,), bool>,
}

impl<IN> Filter<IN>
where
    IN: 'static + ArconType,
{
    pub fn new(udf: &'static dyn for<'r> SafelySendableFn<(&'r IN,), bool>) -> Self {
        Filter { udf }
    }

    #[inline]
    pub fn run_udf(&self, element: &IN) -> bool {
        (self.udf)(element)
    }
}

impl<IN> Operator<IN, IN> for Filter<IN>
where
    IN: 'static + ArconType,
{
    fn handle_element(&mut self, element: ArconElement<IN>) -> Option<Vec<ArconEvent<IN>>> {
        if self.run_udf(&(element.data)) {
            Some(vec![ArconEvent::Element(element)])
        } else {
            None
        }
    }

    fn handle_watermark(&mut self, _w: Watermark) -> Option<Vec<ArconEvent<IN>>> {
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
    fn filter_test() {
        fn filter_fn(x: &i32) -> bool {
            x < &8
        }

        let mut filter = Filter::new(&filter_fn);

        let input_one = ArconElement::new(6 as i32);
        let input_two = ArconElement::new(7 as i32);
        let input_three = ArconElement::new(8 as i32);
        let input_four = ArconElement::new(9 as i32);

        match filter.handle_element(input_one).unwrap().get(0).unwrap() {
            ArconEvent::Element(elem) => assert_eq!(elem.data, 6),
            _ => assert!(false),
        }

        match filter.handle_element(input_two).unwrap().get(0).unwrap() {
            ArconEvent::Element(elem) => assert_eq!(elem.data, 7),
            _ => assert!(false),
        }

        assert!(filter.handle_element(input_three).is_none());
        assert!(filter.handle_element(input_four).is_none());
    }
}
