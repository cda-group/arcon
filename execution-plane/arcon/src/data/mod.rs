use crate::error::ArconResult;
use crate::macros::*;
use crate::messages::protobuf::messages::ArconNetworkMessage;
use crate::messages::protobuf::*;
use crate::streaming::node::NodeID;
use arcon_messages::protobuf::ArconNetworkMessage_oneof_payload::*;
use serde::de::DeserializeOwned;
use serde::*;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

/// Type that can be passed through the Arcon runtime
pub trait ArconType: Sync + Send + Copy + Clone + Debug + Serialize + DeserializeOwned {}

/// Wrapper for unifying passing of Elements and other stream messages (watermarks)
#[derive(Clone, Debug, Copy)]
pub enum ArconEvent<A: ArconType> {
    Element(ArconElement<A>),
    Watermark(Watermark),
    Epoch(Epoch),
}

#[derive(Clone, Debug)]
pub struct ArconMessage<A: ArconType> {
    pub event: ArconEvent<A>,
    pub sender: NodeID,
}

// Convenience methods, "message factories"
impl<A: ArconType> ArconMessage<A> {
    pub fn watermark(timestamp: u64, sender: NodeID) -> ArconMessage<A> {
        ArconMessage {
            event: ArconEvent::<A>::Watermark(Watermark { timestamp }),
            sender,
        }
    }
    pub fn epoch(epoch: u64, sender: NodeID) -> ArconMessage<A> {
        ArconMessage {
            event: ArconEvent::<A>::Epoch(Epoch { epoch }),
            sender,
        }
    }
    pub fn element(data: A, timestamp: Option<u64>, sender: NodeID) -> ArconMessage<A> {
        ArconMessage {
            event: ArconEvent::Element(ArconElement { data, timestamp }),
            sender,
        }
    }
}

/// Watermark
#[derive(Clone, Debug, Copy)]
pub struct Watermark {
    pub timestamp: u64,
}

impl Watermark {
    pub fn new(timestamp: u64) -> Self {
        Watermark { timestamp }
    }
}

/// Epoch
#[derive(Clone, Debug, Copy)]
pub struct Epoch {
    pub epoch: u64,
}

impl Epoch {
    pub fn new(epoch: u64) -> Self {
        Epoch { epoch }
    }
}

impl<A: ArconType> ArconMessage<A> {
    pub fn to_remote(&self) -> ArconResult<ArconNetworkMessage> {
        match &self.event {
            ArconEvent::Element(e) => {
                let serialised_event: Vec<u8> = bincode::serialize(&e.data).map_err(|e| {
                    arcon_err_kind!("Failed to serialise event with err {}", e.to_string())
                })?;

                let timestamp = e.timestamp.unwrap_or(0);
                // TODO: Add Sender info to the ArconNetworkMessage
                Ok(create_element(serialised_event, timestamp))
            }
            ArconEvent::Watermark(w) => {
                let mut msg = ArconNetworkMessage::new();
                let mut msg_watermark = messages::Watermark::new();
                msg_watermark.set_timestamp(w.timestamp);
                msg.set_watermark(msg_watermark);
                msg.set_sender(self.sender.id);
                Ok(msg)
            }
            ArconEvent::Epoch(e) => {
                let mut msg = ArconNetworkMessage::new();
                let mut msg_checkpoint = messages::Checkpoint::new();
                msg_checkpoint.set_epoch(e.epoch);
                msg.set_checkpoint(msg_checkpoint);
                msg.set_sender(self.sender.id);
                Ok(msg)
            }
        }
    }

    pub fn from_remote(message: ArconNetworkMessage) -> ArconResult<Self> {
        let sender = message.get_sender();
        let payload = message.payload.unwrap();
        match payload {
            element(e) => {
                let data: A = bincode::deserialize(e.get_data()).map_err(|e| {
                    arcon_err_kind!("Failed to deserialise event with err {}", e.to_string())
                })?;
                Ok(ArconMessage::<A>::element(
                    data,
                    Some(e.get_timestamp()),
                    sender.into(),
                ))
            }
            watermark(w) => Ok(ArconMessage::watermark(w.timestamp, sender.into())),
            checkpoint(c) => Ok(ArconMessage::epoch(c.epoch, sender.into())),
        }
    }
}

/// A stream element that contains an `ArconType` and an optional timestamp
#[derive(Clone, Debug, Copy)]
pub struct ArconElement<A>
where
    A: ArconType,
{
    pub data: A,
    pub timestamp: Option<u64>,
}

impl<A> ArconElement<A>
where
    A: ArconType,
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

/// Float wrappers for Arcon
///
/// The `Hash` impl rounds the floats down to an integer
/// and then hashes it. Might want to change this later on..
#[arcon]
pub struct ArconF32(f32);

impl Hash for ArconF32 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let s: u64 = self.0.trunc() as u64;
        s.hash(state);
    }
}

impl std::str::FromStr for ArconF32 {
    type Err = ::std::num::ParseFloatError;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let f: f32 = s.parse::<f32>()?;
        Ok(ArconF32(f))
    }
}

impl Deref for ArconF32 {
    type Target = f32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[arcon]
pub struct ArconF64(f64);

impl Hash for ArconF64 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let s: u64 = self.0.trunc() as u64;
        s.hash(state);
    }
}

impl std::str::FromStr for ArconF64 {
    type Err = ::std::num::ParseFloatError;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let f: f64 = s.parse::<f64>()?;
        Ok(ArconF64(f))
    }
}

impl Deref for ArconF64 {
    type Target = f64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Implements `ArconType` for all data types that are supported
impl ArconType for u8 {}
impl ArconType for u16 {}
impl ArconType for u32 {}
impl ArconType for u64 {}
impl ArconType for i8 {}
impl ArconType for i16 {}
impl ArconType for i32 {}
impl ArconType for i64 {}
impl ArconType for f32 {}
impl ArconType for f64 {}
impl ArconType for bool {}
//impl<A: ArconType> ArconType for Vec<A> {}
