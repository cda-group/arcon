// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use super::Epoch;
use kompact::prelude::ActorPath;
use std::{cmp::Ordering, sync::Arc};

/// An Arcon pipeline may have multiple stateful nodes
/// (e.g., Window and Map), thus we must be able to differentiate between them.
pub type KeyedStateID = String;

/// A Key Range with a start and end position
#[derive(Debug, PartialEq, Clone)]
pub struct KeyRange {
    /// Start of the Key Range
    pub start: usize,
    /// End of the Key Range
    pub end: usize,
}

impl KeyRange {
    /// Creates a new KeyRange
    pub fn new(start: usize, end: usize) -> KeyRange {
        assert!(start <= end);
        KeyRange { start, end }
    }
}

/// A Leaf in the Merkle Tree
#[derive(Debug, PartialEq, Clone)]
pub struct TreeLeaf {
    /// Key Range for this TreeLeaf
    pub key_range: KeyRange,
    /// Current Epoch of this TreeLeaf
    pub epoch: Epoch,
    /// Current Actor responsible for this Leaf
    pub source: ActorPath,
}

impl PartialOrd for TreeLeaf {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.key_range.start.partial_cmp(&other.key_range.start)
    }
}

impl TreeLeaf {
    /// Creates a new TreeLeaf
    pub fn new(key_range: KeyRange, source: ActorPath) -> TreeLeaf {
        TreeLeaf {
            key_range,
            epoch: Epoch::new(0),
            source,
        }
    }
    /// Update KeyRange
    pub fn update_range(&mut self, start: usize, end: usize) {
        assert!(start > end);
        self.key_range.start = start;
        self.key_range.end = end;
    }
    /// Update the Leaf's internal epoch
    pub fn update_epoch(&mut self, epoch: Epoch) {
        // Ensure we are actually increasing the epoch
        assert!(self.epoch.epoch < epoch.epoch);

        self.epoch = epoch;
    }
    /// Set new ActorPath to this TreeLeaf
    ///
    /// Is meant to be used for scaling/failure scenarios
    /// where a new Component takes over a KeyRange
    pub fn update_source(&mut self, actor_path: ActorPath) {
        self.source = actor_path;
    }
}

pub struct TreeNode {
    pub data: Option<TreeLeaf>,
}

// TODO: base it on Cassandras
// https://github.com/odnoklassniki/apache-cassandra/blob/master/src/java/org/apache/cassandra/utils/MerkleTree.java
/// Each MerkleTree is responsible for a specific KeyedStateID
///
/// We could for example devide the tree into 4 partitions with a max range of 128 into the
/// following sub ranges: KR(0-32), KR(32-64), KR(64-96), KR(96-128)
///
/// Each TreeLeaf holds a Key Range, Epoch number, and an ActorPath to the operating component.
///
/// ```text
///         root = h1234 = h(h12 + h34)
///        /                          \
///  h12 = h(h1 + h2)           h34 = h(h3 + h4)
///    /            \                 /        \
/// h1 = h(TreeLeaf)  h2 = h(..)   h3 = h(..)  h4 = h(..)
/// ```
///
/// TODO: Implementation should be efficient to serialise
#[derive(Debug)]
pub struct MerkleTree {
    hash_depth: usize,
    /// Max number of KeyRange's
    max_size: usize,
    /// Number of KeyRange's currently in use
    size: usize,
    /// Hash of root
    root_hash: u64,
    //children: Vec<TreeNode>,
}

impl MerkleTree {
    pub fn new() -> MerkleTree {
        MerkleTree {
            hash_depth: 128,
            max_size: 128,
            root_hash: 0,
            size: 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256, Sha512};
}
