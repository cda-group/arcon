use crate::error::ArconResult;
use crate::macros::*;
use abomonation::Abomonation;
use bytes::IntoBuf;
use kompact::prelude::*;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::Hash;

/// Type that can be passed through the Arcon runtime
pub trait ArconType:
    Clone + Debug + Sync + Send + prost::Message + Default + Abomonation + 'static
where
    Self: std::marker::Sized,
{
    /// Encodes `ArconType` into serialised Protobuf data.
    fn encode_storage(&self) -> ArconResult<Vec<u8>> {
        let mut buf = Vec::new();
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

/// Watermark
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

/// Epoch
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

/// Wrapper for unifying passing of Elements and other stream messages (watermarks)
#[derive(Clone, Debug, Abomonation)]
pub enum ArconEvent<A: ArconType> {
    Element(ArconElement<A>),
    Watermark(Watermark),
    Epoch(Epoch),
}

#[derive(prost::Message, Eq, Hash, Copy, Clone, Abomonation)]
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
impl Ord for NodeID {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}
impl PartialOrd for NodeID {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for NodeID {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

#[derive(Clone, Debug, Abomonation)]
pub struct ArconMessage<A: ArconType> {
    pub event: ArconEvent<A>,
    pub sender: NodeID,
}

pub(crate) mod reliable_remote {
    use super::{ArconEvent, ArconMessage, ArconType, Epoch, NodeID, Watermark};
    use bytes::IntoBuf;
    use kompact::prelude::*;
    use prost::*;

    #[derive(prost::Message, Clone)]
    pub struct ArconNetworkMessage {
        #[prost(message, tag = "1")]
        pub sender: Option<NodeID>,
        #[prost(bytes, tag = "2")]
        pub data: Vec<u8>,
        #[prost(message, tag = "3")]
        pub timestamp: Option<u64>,
        #[prost(enumeration = "Payload", tag = "4")]
        pub payload: i32,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Enumeration)]
    pub enum Payload {
        Element = 0,
        Watermark = 1,
        Epoch = 2,
    }

    #[derive(Clone)]
    pub struct ReliableSerde<A: ArconType> {
        pub marker: std::marker::PhantomData<A>,
    }

    impl<A: ArconType> ReliableSerde<A> {
        const SID: kompact::prelude::SerId = 25;
        pub fn new() -> Self {
            ReliableSerde {
                marker: std::marker::PhantomData,
            }
        }
    }
    impl<A: ArconType> Deserialiser<ArconMessage<A>> for ReliableSerde<A> {
        const SER_ID: SerId = Self::SID;

        fn deserialise(buf: &mut Buf) -> Result<ArconMessage<A>, SerError> {
            let mut res = ArconNetworkMessage::decode(buf.bytes()).map_err(|_| {
                SerError::InvalidData("Failed to decode ArconNetworkMessage".to_string())
            })?;

            match res.payload {
                _ if res.payload == Payload::Element as i32 => {
                    let element: A = A::decode_storage(&mut res.data).map_err(|_| {
                        SerError::InvalidData("Failed to decode ArconType".to_string())
                    })?;
                    Ok(ArconMessage::<A>::element(
                        element,
                        res.timestamp,
                        res.sender.unwrap(),
                    ))
                }
                _ if res.payload == Payload::Watermark as i32 => {
                    let mut buf = res.data.into_buf();
                    let wm = Watermark::decode(&mut buf).map_err(|_| {
                        SerError::InvalidData("Failed to decode Watermark".to_string())
                    })?;
                    Ok(ArconMessage::<A> {
                        event: ArconEvent::<A>::Watermark(wm),
                        sender: res.sender.unwrap(),
                    })
                }
                _ if res.payload == Payload::Epoch as i32 => {
                    let mut buf = res.data.into_buf();
                    let epoch = Epoch::decode(&mut buf)
                        .map_err(|_| SerError::InvalidData("Failed to decode Epoch".to_string()))?;
                    Ok(ArconMessage::<A> {
                        event: ArconEvent::<A>::Epoch(epoch),
                        sender: res.sender.unwrap(),
                    })
                }
                _ => {
                    panic!("Something went wrong while deserialising payload");
                }
            }
        }
    }
    impl<A: ArconType> Serialiser<ArconMessage<A>> for ReliableSerde<A> {
        fn ser_id(&self) -> u64 {
            Self::SID
        }
        fn size_hint(&self) -> Option<usize> {
            Some(std::mem::size_of::<ArconNetworkMessage>())
        }
        fn serialise(&self, msg: &ArconMessage<A>, buf: &mut dyn BufMut) -> Result<(), SerError> {
            // TODO: Quite inefficient as of now. fix..
            match &msg.event {
                ArconEvent::Element(elem) => {
                    let remote_msg = ArconNetworkMessage {
                        sender: Some(msg.sender),
                        data: elem.data.encode_storage().unwrap(),
                        timestamp: elem.timestamp,
                        payload: Payload::Element as i32,
                    };
                    let mut data_buf = Vec::new();
                    let _ = remote_msg.encode(&mut data_buf).map_err(|_| {
                        SerError::InvalidData("Failed to encode network message".to_string())
                    })?;
                    buf.put_slice(&data_buf);
                }
                ArconEvent::Watermark(wm) => {
                    let mut wm_buf = Vec::new();
                    let _ = wm.encode(&mut wm_buf).map_err(|_| {
                        SerError::InvalidData(
                            "Failed to encode Watermark for remote msg".to_string(),
                        )
                    })?;
                    let remote_msg = ArconNetworkMessage {
                        sender: Some(msg.sender),
                        data: wm_buf,
                        timestamp: Some(wm.timestamp),
                        payload: Payload::Watermark as i32,
                    };
                    let mut data_buf = Vec::new();
                    let _ = remote_msg.encode(&mut data_buf).map_err(|_| {
                        SerError::InvalidData("Failed to encode network message".to_string())
                    })?;
                    buf.put_slice(&data_buf);
                }
                ArconEvent::Epoch(e) => {
                    let mut epoch_buf = Vec::new();
                    let _ = e.encode(&mut epoch_buf).map_err(|_| {
                        SerError::InvalidData("Failed to encode Epoch for remote msg".to_string())
                    })?;
                    let remote_msg = ArconNetworkMessage {
                        sender: Some(msg.sender),
                        data: epoch_buf,
                        timestamp: None,
                        payload: Payload::Epoch as i32,
                    };
                    let mut data_buf = Vec::new();
                    let _ = remote_msg.encode(&mut data_buf).map_err(|_| {
                        SerError::InvalidData("Failed to encode network message".to_string())
                    })?;
                    buf.put_slice(&data_buf);
                }
            }
            Ok(())
        }
    }
}

pub(crate) mod unsafe_remote {
    use super::{ArconMessage, ArconType};
    use kompact::prelude::*;

    #[derive(Clone)]
    pub struct UnsafeSerde<A: ArconType> {
        marker: std::marker::PhantomData<A>,
    }

    impl<A: ArconType> UnsafeSerde<A> {
        const SID: kompact::prelude::SerId = 26;
        pub fn new() -> Self {
            UnsafeSerde {
                marker: std::marker::PhantomData,
            }
        }
    }

    impl<A: ArconType> Deserialiser<ArconMessage<A>> for UnsafeSerde<A> {
        const SER_ID: SerId = Self::SID;

        fn deserialise(buf: &mut Buf) -> Result<ArconMessage<A>, SerError> {
            // TODO: improve
            let mut bytes = buf.bytes();
            let mut tmp_buf: Vec<u8> = Vec::with_capacity(bytes.len());
            tmp_buf.put_slice(&mut bytes);
            if let Some((msg, _)) = unsafe { abomonation::decode::<ArconMessage<A>>(&mut tmp_buf) }
            {
                Ok(msg.clone())
            } else {
                Err(SerError::InvalidData(
                    "Failed to decode flight data".to_string(),
                ))
            }
        }
    }

    impl<A: ArconType> Serialiser<ArconMessage<A>> for UnsafeSerde<A> {
        fn ser_id(&self) -> u64 {
            Self::SID
        }
        fn size_hint(&self) -> Option<usize> {
            Some(std::mem::size_of::<ArconMessage<A>>())
        }
        fn serialise(&self, msg: &ArconMessage<A>, buf: &mut dyn BufMut) -> Result<(), SerError> {
            // TODO: improve...
            let mut bytes: Vec<u8> = Vec::new();
            let _ = unsafe {
                abomonation::encode(msg, &mut bytes).map_err(|_| {
                    SerError::InvalidData("Failed to encode flight data".to_string())
                })?;
            };
            let remaining = buf.remaining_mut();
            if bytes.len() > remaining {
                unsafe { buf.advance_mut(bytes.len() - remaining) };
                buf.put_slice(&bytes);
            } else {
                buf.put_slice(&bytes);
            }
            Ok(())
        }
    }
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

/// A stream element that contains an `ArconType` and an optional timestamp
#[derive(Clone, Debug, Abomonation)]
pub struct ArconElement<A: ArconType> {
    pub data: A,
    pub timestamp: Option<u64>,
}

impl<A: ArconType> ArconElement<A> {
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

/// Implements `ArconType` for all data types that are supported
impl ArconType for u32 {}
impl ArconType for u64 {}
impl ArconType for i32 {}
impl ArconType for i64 {}
impl ArconType for f32 {}
impl ArconType for f64 {}
impl ArconType for bool {}
impl ArconType for String {}

#[cfg(test)]
mod test {
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
    fn arcon_type_serder_test() {
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
