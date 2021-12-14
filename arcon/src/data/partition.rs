use prost::*;
use std::hash::Hash;

/// Defines the total amount of keys in the key space
const MAX_KEY: u32 = 65535;

pub fn create_shards(total: u32) -> Vec<Shard> {
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
            Shard::new(index, KeyRange::new(start as u32, end as u32))
        })
        .collect()
}

// Maps the data and its key to the shard responsible for it
#[inline]
pub fn shard_lookup<K>(data: &K, total_shards: u32) -> u32
where
    K: Hash + ?Sized,
{
    let mut hasher = arcon_util::key_hasher();
    data.hash(&mut hasher);

    let key = hasher.finish32() % MAX_KEY;
    key * total_shards / MAX_KEY
}

/// A Shard is responsible for a contiguous range of keys
#[derive(Debug)]
pub struct Shard {
    /// Shard Identifier
    id: u32,
    /// Range of keys the shard is responsible for
    range: KeyRange,
}

impl Shard {
    pub fn new(id: u32, range: KeyRange) -> Self {
        Self { id, range }
    }
}

/// A Key Range with a start and end position
#[derive(Message, PartialEq, Clone)]
pub struct KeyRange {
    /// Start of the Key Range
    #[prost(uint32)]
    pub start: u32,
    /// End of the Key Range
    #[prost(uint32)]
    pub end: u32,
}

impl KeyRange {
    /// Creates a new KeyRange
    pub fn new(start: u32, end: u32) -> KeyRange {
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
        let total_shards = shards.len() as u32;

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
