// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::data::ArconType;

#[cfg(feature = "kafka")]
pub mod kafka;
pub mod local_file;
pub mod schema;

//#[cfg(feature = "socket")]
//pub mod socket;

/// Enum containing Poll responses for an Arcon source
#[derive(Debug, Clone)]
pub enum Poll<A> {
    /// Makes the value `A` available
    Ready(A),
    /// Tells the runtime there is currently no records to process
    Pending,
    /// Indicates that the source is finished
    Done,
    /// An error occured while polling
    Error(String),
}

/// Defines an Arcon Source and the methods it must implement
pub trait Source: Send + 'static {
    type Item: ArconType;
    /// Poll Source for an Item
    fn poll_next(&mut self) -> Poll<Self::Item>;
    /// Set offset for the source
    ///
    /// May be used by replayable sources to set a certain offset..
    fn set_offset(&mut self, offset: usize);
}

// Implement Source for IntoIterator<Item = ArconType>
impl<D, I> Source for I
where
    I: IntoIterator<Item = D> + Iterator<Item = D> + Send + 'static,
    D: ArconType,
{
    type Item = D;

    fn poll_next(&mut self) -> Poll<Self::Item> {
        match self.next() {
            Some(item) => Poll::Ready(item),
            None => Poll::Done,
        }
    }
    fn set_offset(&mut self, _: usize) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iterator_source_test() {
        fn sum(mut s: impl Source<Item = u32>) -> u32 {
            let mut sum = 0;
            while let Poll::Ready(v) = s.poll_next() {
                sum += v;
            }
            sum
        }
        let v: Vec<u32> = vec![1, 2, 3, 4];
        let sum = sum(v.into_iter());
        assert_eq!(sum, 10);
    }
}
