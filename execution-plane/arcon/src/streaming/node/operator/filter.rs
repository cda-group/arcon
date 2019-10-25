use crate::prelude::*;
use std::marker::PhantomData;

/// IN: Input Event
pub struct Filter<IN>
where
    IN: 'static + ArconType,
{
    _in: PhantomData<IN>,
    udf: &'static Fn(&IN) -> bool,
}

impl<IN> Filter<IN>
where
    IN: 'static + ArconType,
{
    pub fn new(udf: &'static Fn(&IN) -> bool) -> Self {
        Filter {
            _in: PhantomData,
            udf,
        }
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
            return Ok(vec![ArconEvent::Element(element)]);
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
