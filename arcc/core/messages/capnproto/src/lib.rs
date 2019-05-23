#![feature(custom_attribute)]
extern crate capnp;
extern crate kompact;
#[macro_use]
extern crate derivative;

pub mod messages_capnp {
    include!(concat!(env!("OUT_DIR"), "/messages_capnp.rs"));
}

pub mod builder;
pub mod reader;

use kompact::prelude::BufMut;
use kompact::*;

pub type TaskBuilder = capnp::message::Builder<capnp::message::HeapAllocator>;
pub type TypedTaskReader =
    capnp::message::TypedReader<capnp::serialize::OwnedSegments, messages_capnp::task_msg::Owned>;


#[derive(Derivative)]
#[derivative(Debug)]
pub struct TaskMsg { 
    #[derivative(Debug="ignore")]
    pub b: TaskBuilder,
}

pub struct CapnpSer;

impl Serialisable for TaskMsg {
    fn serid(&self) -> u64 {
        36 // TODO
    }
    fn size_hint(&self) -> Option<usize> {
        None
    }
    fn serialise(&self, buf: &mut BufMut) -> Result<(), SerError> {
        let bytes = builder::serialize_builder(&self.b)
            .map_err(|err| SerError::InvalidData(err.to_string()))?;
        buf.put_slice(&bytes);
        Ok(())
    }
    fn local(self: Box<Self>) -> Result<Box<Any + Send>, Box<Serialisable>> {
        Ok(Box::new(self.b))
    }
}

impl Deserialiser<TypedTaskReader> for CapnpSer {
    fn deserialise(buf: &mut Buf) -> Result<TypedTaskReader, SerError> {
        let deser = reader::deserialize_task_msg(buf.bytes().to_vec())
            .map_err(|err| SerError::InvalidData(err.to_string()))?;
        Ok(deser)
    }
}
