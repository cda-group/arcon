//pub mod collection;
pub mod local_file;
#[cfg(feature = "socket")]
pub mod socket;

#[cfg(feature = "kafka")]
pub mod kafka;
