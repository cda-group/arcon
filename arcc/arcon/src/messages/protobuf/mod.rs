extern crate protobuf;

pub mod messages;

pub use self::messages::*;
use kompact::prelude::BufMut;
use kompact::*;
use protobuf::Message;

pub struct ProtoSer;

impl Serialisable for StreamTaskMessage {
    fn serid(&self) -> u64 {
        serialisation_ids::PBUF
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

impl Deserialiser<StreamTaskMessage> for ProtoSer {
    fn deserialise(buf: &mut Buf) -> Result<StreamTaskMessage, SerError> {
        let parsed = protobuf::parse_from_bytes(buf.bytes())
            .map_err(|err| SerError::InvalidData(err.to_string()))?;
        Ok(parsed)
    }
}

pub fn create_element(data: Vec<u8>, ts: u64) -> StreamTaskMessage {
    let mut msg = StreamTaskMessage::new();
    let mut element = Element::new();
    element.set_data(data);
    element.set_timestamp(ts);

    msg.set_element(element);
    msg
}

pub fn create_keyed_element(data: Vec<u8>, ts: u64, key: u64) -> StreamTaskMessage {
    let mut msg = StreamTaskMessage::new();
    let mut keyed = KeyedElement::new();
    keyed.set_data(data);
    keyed.set_key(key);
    keyed.set_timestamp(ts);

    msg.set_keyed_element(keyed);
    msg
}
