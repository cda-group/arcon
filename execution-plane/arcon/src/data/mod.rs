pub mod serde;

use crate::error::ArconResult;
use crate::macros::*;
use abomonation::Abomonation;
use bytes::IntoBuf;
use kompact::prelude::*;
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

#[derive(Clone, Debug, Abomonation)]
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
