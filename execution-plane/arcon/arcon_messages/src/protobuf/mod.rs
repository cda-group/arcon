extern crate protobuf;

pub mod messages;

pub use self::messages::*;
use kompact::prelude::*;
use protobuf::Message;

pub struct ProtoSer;
impl ProtoSer {
    const SID: SerId = 21;
}

impl Serialisable for ArconNetworkMessage {
    fn ser_id(&self) -> u64 {
        21
    }
    fn size_hint(&self) -> Option<usize> {
        if let Ok(bytes) = self.write_to_bytes() {
            Some(bytes.len())
        } else {
            None
        }
    }
    fn serialise(&self, buf: &mut BufMut) -> Result<(), SerError> {
        let bytes = self
            .write_to_bytes()
            .map_err(|err| SerError::InvalidData(err.to_string()))?;
        buf.put_slice(&bytes);
        Ok(())
    }
    fn local(self: Box<Self>) -> Result<Box<Any + Send>, Box<Serialisable>> {
        Ok(self)
    }
}

impl Deserialiser<ArconNetworkMessage> for ProtoSer {
    const SER_ID: SerId = Self::SID;

    fn deserialise(buf: &mut Buf) -> Result<ArconNetworkMessage, SerError> {
        let parsed = protobuf::parse_from_bytes(buf.bytes())
            .map_err(|err| SerError::InvalidData(err.to_string()))?;
        Ok(parsed)
    }
}

pub fn create_element(data: Vec<u8>, ts: u64) -> ArconNetworkMessage {
    let mut msg = ArconNetworkMessage::new();
    let mut element = Element::new();
    element.set_data(data);
    element.set_timestamp(ts);

    msg.set_element(element);
    msg
}
