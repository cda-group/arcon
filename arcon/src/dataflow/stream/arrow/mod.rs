use crate::data::arrow::ToArrow;
use crate::data::ArconType;
use crate::dataflow::stream::Stream;

/// A Stream that supports Arrow analytics
pub struct ArrowStream<T: ArconType + ToArrow> {
    pub(crate) stream: Stream<T>,
}

impl<T: ArconType + ToArrow> ArrowStream<T> {
    pub(super) fn from(stream: Stream<T>) -> Self {
        Self { stream }
    }
}
