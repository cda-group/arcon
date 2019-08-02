use crate::messages::capnproto::TaskBuilder;
use crate::messages_capnp::*;
use std::error;

pub fn serialize_builder(builder: &TaskBuilder) -> Result<Vec<u8>, Box<error::Error>> {
    let mut buffer = Vec::new();
    capnp::serialize_packed::write_message(&mut buffer, builder)?;
    Ok(buffer)
}
