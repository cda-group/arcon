use crate::backend::serialization::fixed_bytes::FixedBytes;

pub trait Value: prost::Message + Default + Clone + 'static {}
impl<T> Value for T where T: prost::Message + Default + Clone + 'static {}

pub trait Key: prost::Message + Default + Clone + 'static {}
impl<T> Key for T where T: prost::Message + Default + Clone + 'static {}

pub trait Metakey: FixedBytes + Copy + Clone + Send + Sync + 'static {}
impl<T> Metakey for T where T: FixedBytes + Copy + Clone + Send + Sync + 'static {}
