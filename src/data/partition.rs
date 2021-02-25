use super::NodeID;
use prost::*;

pub struct PartitionGroup {
    /// Holds information about how a keyed stream is partitioned.
    ///
    /// One Arcon node per KeyRange
    ///
    /// Example: ((1, KeyRange(0, 31), (2, KeyRange(32, 64), ....)
    ranges: Vec<(NodeID, KeyRange)>,
}

impl PartitionGroup {
    pub fn new(ranges: Vec<(NodeID, KeyRange)>) -> Self {
        Self { ranges }
    }
}

/// A Key Range with a start and end position
#[derive(Message, PartialEq, Clone)]
pub struct KeyRange {
    /// Start of the Key Range
    #[prost(uint64, tag = "1")]
    pub start: u64,
    /// End of the Key Range
    #[prost(uint64, tag = "2")]
    pub end: u64,
}

impl KeyRange {
    /// Creates a new KeyRange
    pub fn new(start: u64, end: u64) -> KeyRange {
        assert!(start < end, "start range has to be smaller than end range");
        KeyRange { start, end }
    }
}
