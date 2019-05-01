use crate::messages_capnp::*;
use crate::*;
use std::error;

pub fn deserialize_task_msg(bytes: Vec<u8>) -> Result<TypedTaskReader, Box<error::Error>> {
    let basic_reader = deserialize(&bytes)?;
    let task_reader: TypedTaskReader = capnp::message::TypedReader::new(basic_reader);
    Ok(task_reader)
}

pub fn deserialize(
    bytes: &Vec<u8>,
) -> Result<capnp::message::Reader<capnp::serialize::OwnedSegments>, Box<error::Error>> {
    let reader = capnp::serialize_packed::read_message(
        &mut bytes.as_slice(),
        capnp::message::ReaderOptions::new(),
    )?;
    Ok(reader)
}
