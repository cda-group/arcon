pub mod collection;
pub mod local_file;
pub mod socket;

#[cfg(feature = "kafka")]
pub mod kafka;