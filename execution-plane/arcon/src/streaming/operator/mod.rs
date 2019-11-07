pub mod filter;
pub mod flatmap;
pub mod map;

pub use filter::Filter;
pub use flatmap::FlatMap;
pub use map::Map;

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
