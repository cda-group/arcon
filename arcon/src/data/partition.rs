use prost::*;
use std::hash::Hash;

/// Defines the total amount of keys in the key space
const MAX_KEY: u64 = 65535;

pub fn create_shards(total: u64) -> Vec<Shard> {
    assert!(
        total < MAX_KEY,
        "Attempted to create more shards than allowed keys {}",
        MAX_KEY
    );
    (0..total)
        .into_iter()
        .map(|index| {
            let start = (index * MAX_KEY + total - 1) / total;
            let end = ((index + 1) * MAX_KEY - 1) / total;
            Shard::new(index, KeyRange::new(start, end))
        })
        .collect()
}

pub fn shard_lookup<K>(data: &K, total_shards: u64) -> u64
where
    K: Hash + ?Sized,
{
    let mut hasher = arcon_util::key_hasher();
    data.hash(&mut hasher);

    shard_lookup_with_key(hasher.finish32() as u64, total_shards)
}

#[inline]
pub fn shard_lookup_with_key(
    hashed_key: u64,
    total_shards: u64,
) -> u64 {
    let key = hashed_key % MAX_KEY;
    key * total_shards / MAX_KEY
}

/// A Shard is responsible for a contiguous range of keys
#[derive(Debug)]
pub struct Shard {
    /// Shard Identifier
    id: u64,
    /// Range of keys the shard is responsible for
    range: KeyRange,
}

impl Shard {
    pub fn new(id: u64, range: KeyRange) -> Self {
        Self { id, range }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_test() {
        let shards = create_shards(4);
        assert_eq!(shards[0].range, KeyRange::new(0, 16383));
        assert_eq!(shards[1].range, KeyRange::new(16384, 32767));
        assert_eq!(shards[2].range, KeyRange::new(32768, 49151));
        assert_eq!(shards[3].range, KeyRange::new(49152, 65534));
    }

    #[test]
    fn shard_lookup_test() {
        let shards = create_shards(4);
        let total_shards = shards.len() as u64;

        let s1 = shard_lookup("a", total_shards);
        let s2 = shard_lookup("a", total_shards);
        assert_eq!(s1, s2);

        let s3 = shard_lookup("b", total_shards);
        assert_ne!(s2, s3);

        // verify tuples work..
        assert_eq!(
            shard_lookup(&(10, "user"), total_shards),
            shard_lookup(&(10, "user"), total_shards)
        );
        assert_ne!(
            shard_lookup(&(10, "user"), total_shards),
            shard_lookup(&(11, "other_user"), total_shards)
        );

        let mut hit: [bool; 4] = [false; 4];

        // all shards ought to be hit
        for i in 0..100 {
            let shard = shard_lookup(&i, total_shards);
            hit[shard as usize] = true;
        }
        assert_eq!(hit, [true, true, true, true]);
    }
}
