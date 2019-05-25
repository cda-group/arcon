use crate::messages_capnp::*;
use crate::TaskBuilder;
use std::error;

pub fn serialize_builder(builder: &TaskBuilder) -> Result<Vec<u8>, Box<error::Error>> {
    let mut buffer = Vec::new();
    capnp::serialize_packed::write_message(&mut buffer, builder)?;
    Ok(buffer)
}
