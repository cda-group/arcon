use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use std::hash::Hash;

#[derive(Clone, Debug, Copy)]
pub struct ArconElement<A>
where
    A: 'static + ArconType,
{
    pub data: A,
    pub timestamp: Option<u64>,
}

impl<A> ArconElement<A>
where
    A: 'static + ArconType,
{
    pub fn new(data: A) -> Self {
        ArconElement {
            data,
            timestamp: None,
        }
    }

    pub fn with_timestamp(data: A, ts: u64) -> Self {
        ArconElement {
            data,
            timestamp: Some(ts),
        }
    }
}

/// Type that can be passed through the Arcon runtime
pub trait ArconType:
    Sync + Send + Clone + Copy + Debug + Hash + Serialize + DeserializeOwned
{
}

impl ArconType for u8 {}
impl ArconType for u16 {}
impl ArconType for u32 {}
impl ArconType for u64 {}
impl ArconType for i8 {}
impl ArconType for i16 {}
impl ArconType for i32 {}
impl ArconType for i64 {}

// TODO: does not impl Hash
// impl ArconType for f32 {}
// impl ArconType for f64 {}
