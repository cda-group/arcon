use super::NodeID;
use prost::*;

/// Keyed state in Arcon is split into Regions.
#[derive(Debug)]
pub struct Region {
    /// A Region may internally further split up the key ranges
    /// Example: ((0, KeyRange(0, 31), (1, KeyRange(32, 64), ....)
    ranges: Vec<(NodeID, KeyRange)>,
}

impl Region {
    pub fn new(instances: usize, max_key: usize) -> Self {
        let mut ranges = Vec::new();
        for index in 0..instances {
            let start = (index * max_key + instances - 1) / instances;
            let end = ((index + 1) * max_key - 1) / instances;
            ranges.push((
                NodeID::new(index as u32),
                KeyRange::new(start as u64, end as u64),
            ));
        }

        Self { ranges }
    }
}

/// A Key Range with a start and end position
#[derive(Message, PartialEq, Clone)]
pub struct KeyRange {
    /// Start of the Key Range
    #[prost(uint64)]
    pub start: u64,
    /// End of the Key Range
    #[prost(uint64)]
    pub end: u64,
}

impl KeyRange {
    /// Creates a new KeyRange
    pub fn new(start: u64, end: u64) -> KeyRange {
        assert!(start < end, "start range has to be smaller than end range");
        KeyRange { start, end }
    }
}
