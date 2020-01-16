use crate::data::{ArconElement, ArconEvent, ArconType, Epoch, Watermark};
use crate::streaming::node::operator::Operator;
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
    fn handle_element(&mut self, element: ArconElement<IN>) -> ArconResult<Vec<ArconEvent<IN>>> {
        if self.run_udf(&(element.data)) {
            Ok(vec![ArconEvent::Element(element)])
        } else {
            Ok(Vec::new())
        }
    }

    fn handle_watermark(&mut self, _w: Watermark) -> ArconResult<Vec<ArconEvent<IN>>> {
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

        let r3 = filter.handle_element(input_three).unwrap();
        assert!(r3.is_empty());
        let r4 = filter.handle_element(input_four).unwrap();
        assert!(r4.is_empty());
    }
}
