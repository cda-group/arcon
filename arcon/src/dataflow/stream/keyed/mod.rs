use crate::data::ArconType;
use crate::dataflow::stream::Stream;

/// Represents a stream that has been keyed
pub struct KeyedStream<T: ArconType> {
    pub(crate) stream: Stream<T>,
}

impl<T: ArconType> KeyedStream<T> {
    pub(super) fn from(stream: Stream<T>) -> Self {
        Self { stream }
    }
}
