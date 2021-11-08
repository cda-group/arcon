/// Alias type for Hasher used to shard data in Arcon
///
/// Arcon uses MurmurHash3
#[cfg(feature = "hasher")]
pub type KeyHasher = mur3::Hasher32;

/// Helper function to create [KeyHasher]
#[cfg(feature = "hasher")]
#[inline]
pub fn key_hasher() -> KeyHasher {
    KeyHasher::with_seed(0)
}
