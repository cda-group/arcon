// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::data::ArconType;

pub mod local_file;
//#[cfg(feature = "kafka")]
//pub mod kafka;
//#[cfg(feature = "socket")]
//pub mod socket;

#[derive(Debug, Clone)]
pub enum Poll<A> {
    Ready(A),
    Pending,
    Done,
    Error(String),
}

/// Defines an Arcon Source and the methods it must implement
pub trait Source: Send + 'static {
    type Item: ArconType;
    /// Poll Source for an Item
    ///
    /// `Poll::Ready(v)` makes value `v` available.
    /// `Poll::Pending` tells the runtime that there is currently no records to process.
    /// `Poll::Done` indicates that the source is finished.
    /// `Poll::Error(err)` for reporting errors
    fn poll_next(&mut self) -> Poll<Self::Item>;
    /// Set offset for the source
    ///
    /// May be used by replayble sources to set a certain offset..
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
        if let Some(item) = self.next() {
            Poll::Ready(item)
        } else {
            Poll::Done
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
