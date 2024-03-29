/// Arrow related types
pub mod arrow;
/// Serialisers and Deserialiser for in-flight data
pub mod flight_serde;
#[allow(dead_code)]
pub mod partition;
/// Known Serialisation IDs for Arcon Types
mod ser_id;

use crate::buffer::event::BufferReader;
use kompact::prelude::*;
use prost::{Message, Oneof};
use std::{
    fmt,
    hash::{Hash, Hasher},
    ops::Deref,
};

/// A type alias for registered state within Arcon
pub type StateID = String;

/// A type alias for an ArconType version id
pub type VersionId = u32;

pub trait ArconTypeBounds:
    Clone + fmt::Debug + Sync + Send + prost::Message + Default + 'static
{
}
impl<T> ArconTypeBounds for T where
    T: Clone + fmt::Debug + Sync + Send + prost::Message + Default + 'static
{
}

/// Type that can be passed through the Arcon runtime
pub trait ArconType: ArconTypeBounds
where
    Self: std::marker::Sized,
{
    /// Serialisation ID for Arcon's Reliable In-flight serde
    const RELIABLE_SER_ID: SerId;
    /// Current version of this ArconType
    const VERSION_ID: VersionId;
}

/// An Enum containing all possible stream events that may occur in an execution
#[derive(Oneof, Clone)]
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

#[derive(Message, Clone)]
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
}

impl<A: ArconType> From<ArconEvent<A>> for ArconEventWrapper<A> {
    fn from(inner: ArconEvent<A>) -> Self {
        ArconEventWrapper { inner: Some(inner) }
    }
}

/// A Stream element containing some data and timestamp
#[derive(Message, Clone)]
pub struct ArconElement<A: ArconType> {
    #[prost(message, required, tag = "1")]
    pub data: A,
    #[prost(uint64, tag = "2")]
    pub timestamp: u64,
}

impl<A: ArconType> ArconElement<A> {
    /// Creates an ArconElement without a timestamp
    pub fn new(data: A) -> Self {
        ArconElement { data, timestamp: 0 }
    }

    /// Creates an ArconElement with a timestamp
    pub fn with_timestamp(data: A, timestamp: u64) -> Self {
        ArconElement { data, timestamp }
    }
}

/// Watermark message containing a [u64] timestamp
#[derive(Message, Clone, Copy, Ord, PartialOrd, Eq, PartialEq)]
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
#[derive(Message, Clone, Hash, Copy, Ord, PartialOrd, Eq, PartialEq)]
pub struct Epoch {
    #[prost(uint64, tag = "1")]
    pub epoch: u64,
}

impl Epoch {
    /// Creates a new Epoch
    pub fn new(epoch: u64) -> Self {
        Epoch { epoch }
    }
}

/// Container that holds two possible variants of messages
pub enum MessageContainer<A: ArconType> {
    /// Batch of events backed Rust's system allocator
    ///
    /// Used when receiving events from the network or when restoring messages from a state backend
    Raw(RawArconMessage<A>),
    /// Batch of events backed by Arcon's allocator
    Local(ArconMessage<A>),
}

impl<A: ArconType> MessageContainer<A> {
    /// Return a reference to the sender ID of this message
    #[inline]
    pub fn sender(&self) -> &NodeID {
        match self {
            MessageContainer::Raw(r) => &r.sender,
            MessageContainer::Local(l) => &l.sender,
        }
    }
    /// Return number of events in the message

    #[inline]
    #[cfg(feature = "metrics")]
    pub fn total_events(&self) -> u64 {
        match self {
            MessageContainer::Raw(r) => r.events.len() as u64,
            MessageContainer::Local(l) => l.events.len() as u64,
        }
    }
    /// Consumes the container and returns a RawArconMessage
    pub fn raw(self) -> RawArconMessage<A> {
        match self {
            MessageContainer::Raw(r) => r,
            MessageContainer::Local(l) => l.into(),
        }
    }
}

/// An ArconMessage backed by a reusable EventBuffer
#[derive(Debug, Clone)]
pub struct ArconMessage<A: ArconType> {
    /// Batch of ArconEvents backed by an EventBuffer
    pub events: BufferReader<ArconEventWrapper<A>>,
    /// ID identifying where the message is sent from
    pub sender: NodeID,
}

/// A raw ArconMessage for serialisation
#[derive(Message, Clone)]
pub struct RawArconMessage<A: ArconType> {
    /// Batch of ArconEvents
    #[prost(message, repeated, tag = "1")]
    pub events: Vec<ArconEventWrapper<A>>,
    /// ID identifying where the message is sent from
    #[prost(message, required, tag = "2")]
    pub sender: NodeID,
}

impl<A: ArconType> From<ArconMessage<A>> for RawArconMessage<A> {
    fn from(msg: ArconMessage<A>) -> Self {
        RawArconMessage {
            events: msg.events.to_vec(),
            sender: msg.sender,
        }
    }
}

/// Convenience methods
#[cfg(test)]
impl<A: ArconType> ArconMessage<A> {
    /// Creates an ArconMessage with a single [ArconEvent::Watermark] event
    ///
    /// This function should only be used for development and test purposes.
    pub fn watermark(timestamp: u64, sender: NodeID) -> ArconMessage<A> {
        ArconMessage {
            events: vec![ArconEvent::<A>::Watermark(Watermark { timestamp }).into()].into(),
            sender,
        }
    }
    /// Creates an ArconMessage with a single [ArconEvent::Epoch] event
    ///
    /// This function should only be used for development and test purposes.
    pub fn epoch(epoch: u64, sender: NodeID) -> ArconMessage<A> {
        ArconMessage {
            events: vec![ArconEvent::<A>::Epoch(Epoch { epoch }).into()].into(),
            sender,
        }
    }
    /// Creates an ArconMessage with a single [ArconEvent::Death] event
    ///
    /// This function should only be used for development and test purposes.
    pub fn death(msg: String, sender: NodeID) -> ArconMessage<A> {
        ArconMessage {
            events: vec![ArconEvent::<A>::Death(msg).into()].into(),
            sender,
        }
    }
    /// Creates an ArconMessage with a single [ArconEvent::Element] event
    ///
    /// This function should only be used for development and test purposes.
    pub fn element(data: A, timestamp: u64, sender: NodeID) -> ArconMessage<A> {
        ArconMessage {
            events: vec![ArconEvent::Element(ArconElement { data, timestamp }).into()].into(),
            sender,
        }
    }
}

/// A NodeID is used to identify a message sender
#[derive(Message, PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone)]
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

// Implement ArconType for primitives.
// NOTE: This is mainly for testing and development. In practice,
// an ArconType is always a struct or enum.

impl ArconType for u32 {
    const RELIABLE_SER_ID: SerId = ser_id::RELIABLE_U32_ID;
    const VERSION_ID: VersionId = 1;
}
impl ArconType for u64 {
    const RELIABLE_SER_ID: SerId = ser_id::RELIABLE_U64_ID;
    const VERSION_ID: VersionId = 1;
}
impl ArconType for i32 {
    const RELIABLE_SER_ID: SerId = ser_id::RELIABLE_I32_ID;
    const VERSION_ID: VersionId = 1;
}
impl ArconType for i64 {
    const RELIABLE_SER_ID: SerId = ser_id::RELIABLE_I64_ID;
    const VERSION_ID: VersionId = 1;
}
impl ArconType for ArconF32 {
    const RELIABLE_SER_ID: SerId = ser_id::RELIABLE_F32_ID;
    const VERSION_ID: VersionId = 1;
}
impl ArconType for ArconF64 {
    const RELIABLE_SER_ID: SerId = ser_id::RELIABLE_F64_ID;
    const VERSION_ID: VersionId = 1;
}
impl ArconType for bool {
    const RELIABLE_SER_ID: SerId = ser_id::RELIABLE_BOOLEAN_ID;
    const VERSION_ID: VersionId = 1;
}
impl ArconType for String {
    const RELIABLE_SER_ID: SerId = ser_id::RELIABLE_STRING_ID;
    const VERSION_ID: VersionId = 1;
}

/// Float wrapper for f32 in order to impl Hash [std::hash::Hash]
///
/// The `Hash` impl rounds the floats down to an integer and then hashes it.
#[derive(Clone, Message)]
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
#[derive(Clone, Message)]
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

/// Arcon variant of the `Never` (or `!`) type which fulfills `ArconType` requirements
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ArconNever {}
impl ArconNever {
    pub const IS_UNREACHABLE: &'static str = "The ArconNever type cannot be instantiated!";
}
impl ArconType for ArconNever {
    const RELIABLE_SER_ID: SerId = ser_id::NEVER_ID;
    const VERSION_ID: VersionId = 1;
}
impl fmt::Debug for ArconNever {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        unreachable!("{}", ArconNever::IS_UNREACHABLE);
    }
}
impl prost::Message for ArconNever {
    fn encoded_len(&self) -> usize {
        unreachable!("{}", ArconNever::IS_UNREACHABLE);
    }

    fn clear(&mut self) {
        unreachable!("{}", ArconNever::IS_UNREACHABLE);
    }

    fn encode_raw<B>(&self, _: &mut B)
    where
        B: bytes::buf::BufMut,
    {
        unreachable!("{}", ArconNever::IS_UNREACHABLE);
    }
    fn merge_field<B>(
        &mut self,
        _: u32,
        _: prost::encoding::WireType,
        _: &mut B,
        _: prost::encoding::DecodeContext,
    ) -> std::result::Result<(), prost::DecodeError>
    where
        B: bytes::buf::Buf,
    {
        unreachable!("{}", ArconNever::IS_UNREACHABLE);
    }
}

impl Default for ArconNever {
    fn default() -> Self {
        unreachable!("{}", ArconNever::IS_UNREACHABLE);
    }
}
