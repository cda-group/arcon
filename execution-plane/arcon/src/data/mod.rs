// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

/// Serialisers and Deserialiser for in-flight data
pub mod flight_serde;

use crate::error::ArconResult;
use crate::macros::*;
use abomonation::Abomonation;
use kompact::prelude::*;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

/// Type that can be passed through the Arcon runtime
pub trait ArconType:
    Clone + Debug + Hash + Sync + Send + prost::Message + Default + Abomonation + 'static
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
#[derive(prost::Oneof, Clone, Abomonation)]
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

/// A Stream element containing some data and timestamp
#[derive(prost::Message, Clone, Abomonation)]
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
#[derive(prost::Message, Clone, Copy, Abomonation)]
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
#[derive(prost::Message, Clone, Copy, Abomonation)]
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
#[derive(Debug, Clone, Abomonation)]
pub struct ArconMessage<A: ArconType> {
    /// Buffer of ArconEvents
    pub events: Vec<ArconEvent<A>>,
    /// ID identifying where the message is sent from
    pub sender: NodeID,
}

/// Convenience methods
impl<A: ArconType> ArconMessage<A> {
    /// Creates an ArconMessage with a single [ArconEvent::Watermark] event
    ///
    /// This function should only be used for development and test purposes.
    pub fn watermark(timestamp: u64, sender: NodeID) -> ArconMessage<A> {
        ArconMessage {
            events: vec![ArconEvent::<A>::Watermark(Watermark { timestamp })],
            sender,
        }
    }
    /// Creates an ArconMessage with a single [ArconEvent::Epoch] event
    ///
    /// This function should only be used for development and test purposes.
    pub fn epoch(epoch: u64, sender: NodeID) -> ArconMessage<A> {
        ArconMessage {
            events: vec![ArconEvent::<A>::Epoch(Epoch { epoch })],
            sender,
        }
    }
    /// Creates an ArconMessage with a single [ArconEvent::Death] event
    ///
    /// This function should only be used for development and test purposes.
    pub fn death(msg: String, sender: NodeID) -> ArconMessage<A> {
        ArconMessage {
            events: vec![ArconEvent::<A>::Death(msg)],
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
            })],
            sender,
        }
    }
}

/// A NodeID is used to identify a message sender
#[derive(prost::Message, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone, Abomonation)]
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
#[derive(Clone, prost::Message, Abomonation)]
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
#[derive(Clone, prost::Message, Abomonation)]
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
    #[derive(prost::Message)]
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
