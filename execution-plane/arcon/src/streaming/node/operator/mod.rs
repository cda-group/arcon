pub mod function;
pub mod sink;
pub mod source;
pub mod window;

use crate::data::{ArconElement, ArconEvent, ArconType, Epoch, Watermark};
use arcon_error::ArconResult;

pub trait Operator<IN, OUT>
where
    IN: ArconType,
    OUT: ArconType,
{
    fn handle_element(&mut self, element: ArconElement<IN>) -> ArconResult<Vec<ArconEvent<OUT>>>;
    fn handle_watermark(&mut self, watermark: Watermark) -> ArconResult<Vec<ArconEvent<OUT>>>;
    fn handle_epoch(&mut self, epoch: Epoch) -> ArconResult<Vec<u8>>;
}
