// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

/// Serialisers and Deserialiser for in-flight data
pub mod flight_serde;

use crate::{error::ArconResult, macros::*};
use abomonation::Abomonation;
use kompact::prelude::*;
use prost::{Message as PMessage, Oneof as POneof};
#[cfg(feature = "arcon_serde")]
use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug,
    hash::{Hash, Hasher},
    ops::Deref,
};

pub trait ArconTypeBoundsNoSerde:
    Clone + Debug + Hash + Sync + Send + PMessage + Default + Abomonation + 'static
{
}

impl<T> ArconTypeBoundsNoSerde for T where
    T: Clone + Debug + Hash + Sync + Send + PMessage + Default + Abomonation + 'static
{
}

cfg_if::cfg_if! {
    if #[cfg(feature = "arcon_serde")] {
        pub trait ArconTypeBounds: ArconTypeBoundsNoSerde + Serialize + for<'de> Deserialize<'de> {}
        impl<T> ArconTypeBounds for T where T: ArconTypeBoundsNoSerde + Serialize + for<'de> Deserialize<'de> {}
    } else {
        pub trait ArconTypeBounds: ArconTypeBoundsNoSerde {}
        impl<T> ArconTypeBounds for T where T: ArconTypeBoundsNoSerde {}
    }
}

/// Type that can be passed through the Arcon runtime
pub trait ArconType: ArconTypeBounds
where
    Self: std::marker::Sized,
{
    /// Encodes `ArconType` into serialised Protobuf data.
    fn encode_storage(&self) -> ArconResult<Vec<u8>> {
        let mut buf = Vec::with_capacity(self.encoded_len());
        self.encode(&mut buf).map_err(|e| {
            arcon_err_kind!("Failed to encode ArconType with err {}", e.to_string())
        })?;
        Ok(buf)
    }
    /// Decodes bytes from encoded Protobuf data to `ArconType`
    fn decode_storage(bytes: &mut [u8]) -> ArconResult<Self> {
        let mut buf = bytes.into_buf();
        let res: Self = Self::decode(&mut buf).map_err(|e| {
            arcon_err_kind!("Failed to decode ArconType with err {}", e.to_string())
        })?;
        Ok(res)
    }
}

/// An Enum containing all possible stream events that may occur in an execution
#[cfg_attr(feature = "arcon_serde", derive(Serialize, Deserialize))]
#[derive(POneof, Clone, Abomonation)]
#[cfg_attr(feature = "arcon_serde", serde(bound = "A: ArconType"))]
pub enum ArconEvent<A: ArconType> {
    /// A stream element containing some data of type [ArconType] and an optional timestamp [u64]
    #[prost(message, tag = "1")]
    Element(ArconElement<A>),
    /// A [Watermark] message
    #[prost(message, tag = "2")]
    Watermark(Watermark),
    /// An [Epoch] marker message
    #[prost(message, tag = "3")]
    Epoch(Epoch),
    /// A death message
    #[prost(message, tag = "4")]
    Death(String),
}

// The struct below is required because of the peculiarity of prost/protobuf - you cannot have
// repeated (like, a Vec<_>) oneof fields, so we wrap ArconEvent in a struct which implements
// prost::Message. Unfortunately protobuf also doesn't allow for required oneof fields, so the inner
// value has to be optional. In practice we expect it to always be Some.

#[cfg_attr(feature = "arcon_serde", derive(Serialize, Deserialize))]
#[derive(PMessage, Clone, Abomonation)]
#[cfg_attr(feature = "arcon_serde", serde(bound = "A: ArconType"))]
pub struct ArconEventWrapper<A: ArconType> {
    #[prost(oneof = "ArconEvent::<A>", tags = "1, 2, 3, 4")]
    inner: Option<ArconEvent<A>>,
}

impl<A: ArconType> ArconEventWrapper<A> {
    pub fn unwrap(self) -> ArconEvent<A> {
        self.inner
            .expect("ArconEventWrapper.inner is None. Prost deserialization error?")
    }

    pub fn unwrap_ref(&self) -> &ArconEvent<A> {
        self.inner
            .as_ref()
            .expect("ArconEventWrapper.inner is None. Prost deserialization error?")
    }

    pub fn unwrap_mut(&mut self) -> &mut ArconEvent<A> {
        self.inner
            .as_mut()
            .expect("ArconEventWrapper.inner is None. Prost deserialization error?")
    }
}

impl<A: ArconType> From<ArconEvent<A>> for ArconEventWrapper<A> {
    fn from(inner: ArconEvent<A>) -> Self {
        ArconEventWrapper { inner: Some(inner) }
    }
}

/// A Stream element containing some data and timestamp
#[cfg_attr(feature = "arcon_serde", derive(Serialize, Deserialize))]
#[derive(PMessage, Clone, Abomonation)]
#[cfg_attr(feature = "arcon_serde", serde(bound = "A: ArconType"))]
pub struct ArconElement<A: ArconType> {
    #[prost(message, tag = "1")]
    pub data: Option<A>,
    #[prost(message, tag = "2")]
    pub timestamp: Option<u64>,
}

impl<A: ArconType> ArconElement<A> {
    /// Creates an ArconElement without a timestamp
    pub fn new(data: A) -> Self {
        ArconElement {
            data: Some(data),
            timestamp: None,
        }
    }

    /// Creates an ArconElement with a timestamp
    pub fn with_timestamp(data: A, ts: u64) -> Self {
        ArconElement {
            data: Some(data),
            timestamp: Some(ts),
        }
    }
}

/// Watermark message containing a [u64] timestamp
#[cfg_attr(feature = "arcon_serde", derive(Serialize, Deserialize))]
#[derive(PMessage, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Abomonation)]
pub struct Watermark {
    #[prost(uint64, tag = "1")]
    pub timestamp: u64,
}

impl Watermark {
    pub fn new(timestamp: u64) -> Self {
        Watermark { timestamp }
    }
}

/// Epoch marker message
#[cfg_attr(feature = "arcon_serde", derive(Serialize, Deserialize))]
#[derive(PMessage, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Abomonation)]
pub struct Epoch {
    #[prost(uint64, tag = "1")]
    pub epoch: u64,
}

impl Epoch {
    pub fn new(epoch: u64) -> Self {
        Epoch { epoch }
    }
}

/// An ArconMessage contains a batch of [ArconEvent] and one [NodeID] identifier
#[cfg_attr(feature = "arcon_serde", derive(Serialize, Deserialize))]
#[derive(PMessage, Clone, Abomonation)]
#[cfg_attr(feature = "arcon_serde", serde(bound = "A: ArconType"))]
pub struct ArconMessage<A: ArconType> {
    /// Buffer of ArconEvents
    #[prost(message, repeated, tag = "1")]
    pub events: Vec<ArconEventWrapper<A>>,
    /// ID identifying where the message is sent from
    #[prost(message, required, tag = "5")]
    pub sender: NodeID,
}

/// Convenience methods
impl<A: ArconType> ArconMessage<A> {
    /// Creates an ArconMessage with a single [ArconEvent::Watermark] event
    ///
    /// This function should only be used for development and test purposes.
    pub fn watermark(timestamp: u64, sender: NodeID) -> ArconMessage<A> {
        ArconMessage {
            events: vec![ArconEvent::<A>::Watermark(Watermark { timestamp }).into()],
            sender,
        }
    }
    /// Creates an ArconMessage with a single [ArconEvent::Epoch] event
    ///
    /// This function should only be used for development and test purposes.
    pub fn epoch(epoch: u64, sender: NodeID) -> ArconMessage<A> {
        ArconMessage {
            events: vec![ArconEvent::<A>::Epoch(Epoch { epoch }).into()],
            sender,
        }
    }
    /// Creates an ArconMessage with a single [ArconEvent::Death] event
    ///
    /// This function should only be used for development and test purposes.
    pub fn death(msg: String, sender: NodeID) -> ArconMessage<A> {
        ArconMessage {
            events: vec![ArconEvent::<A>::Death(msg).into()],
            sender,
        }
    }
    /// Creates an ArconMessage with a single [ArconEvent::Element] event
    ///
    /// This function should only be used for development and test purposes.
    pub fn element(data: A, timestamp: Option<u64>, sender: NodeID) -> ArconMessage<A> {
        ArconMessage {
            events: vec![ArconEvent::Element(ArconElement {
                data: Some(data),
                timestamp,
            })
            .into()],
            sender,
        }
    }
}

/// A NodeID is used to identify a message sender
#[cfg_attr(feature = "arcon_serde", derive(Serialize, Deserialize))]
#[derive(PMessage, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone, Abomonation)]
pub struct NodeID {
    #[prost(uint32, tag = "1")]
    pub id: u32,
}

impl NodeID {
    pub fn new(new_id: u32) -> NodeID {
        NodeID { id: new_id }
    }
}

impl From<u32> for NodeID {
    fn from(id: u32) -> Self {
        NodeID::new(id)
    }
}

impl Into<u32> for NodeID {
    fn into(self) -> u32 {
        self.id
    }
}

// Implement ArconType for all data types that are supported
impl ArconType for u32 {}
impl ArconType for u64 {}
impl ArconType for i32 {}
impl ArconType for i64 {}
impl ArconType for ArconF32 {}
impl ArconType for ArconF64 {}
impl ArconType for bool {}
impl ArconType for String {}

/// Float wrapper for f32 in order to impl Hash [std::hash::Hash]
///
/// The `Hash` impl rounds the floats down to an integer and then hashes it.
#[cfg_attr(feature = "arcon_serde", derive(Serialize, Deserialize))]
#[derive(Clone, PMessage, Abomonation)]
#[repr(transparent)]
pub struct ArconF32 {
    #[prost(float, tag = "1")]
    pub value: f32,
}

impl ArconF32 {
    pub fn new(value: f32) -> ArconF32 {
        ArconF32 { value }
    }
}

impl Hash for ArconF32 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let s: u64 = self.value.trunc() as u64;
        s.hash(state);
    }
}

impl From<f32> for ArconF32 {
    fn from(value: f32) -> Self {
        ArconF32::new(value)
    }
}

impl std::str::FromStr for ArconF32 {
    type Err = ::std::num::ParseFloatError;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let f: f32 = s.parse::<f32>()?;
        Ok(ArconF32::new(f))
    }
}
impl Deref for ArconF32 {
    type Target = f32;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl PartialEq for ArconF32 {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

/// Float wrapper for f64 in order to impl Hash [std::hash::Hash]
///
/// The `Hash` impl rounds the floats down to an integer and then hashes it.
#[cfg_attr(feature = "arcon_serde", derive(Serialize, Deserialize))]
#[derive(Clone, PMessage, Abomonation)]
#[repr(transparent)]
pub struct ArconF64 {
    #[prost(double, tag = "1")]
    pub value: f64,
}

impl ArconF64 {
    pub fn new(value: f64) -> ArconF64 {
        ArconF64 { value }
    }
}

impl Hash for ArconF64 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let s: u64 = self.value.trunc() as u64;
        s.hash(state);
    }
}

impl From<f64> for ArconF64 {
    fn from(value: f64) -> Self {
        ArconF64::new(value)
    }
}

impl std::str::FromStr for ArconF64 {
    type Err = ::std::num::ParseFloatError;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let f: f64 = s.parse::<f64>()?;
        Ok(ArconF64::new(f))
    }
}
impl Deref for ArconF64 {
    type Target = f64;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl PartialEq for ArconF64 {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

#[cfg(test)]
pub mod test {
    use super::*;

    #[arcon]
    pub struct ArconDataTest {
        #[prost(uint32, tag = "1")]
        pub id: u32,
        #[prost(uint32, repeated, tag = "2")]
        pub items: Vec<u32>,
    }

    #[test]
    fn arcon_type_serde_test() {
        let items = vec![1, 2, 3, 4, 5, 6, 7];
        let data = ArconDataTest {
            id: 1,
            items: items.clone(),
        };
        let mut bytes = data.encode_storage().unwrap();
        let decoded = ArconDataTest::decode_storage(&mut bytes).unwrap();
        assert_eq!(decoded.items, items);
    }
}
