use crate::error::ErrorKind::*;
use crate::error::*;
use crate::prelude::Serialize;
use crate::streaming::Channel;
use kompact::{ActorPath, ActorRef, ComponentDefinition};
use messages::protobuf::*;
use std::hash::Hash;

pub mod broadcast;
pub mod forward;
pub mod hash;

/// `Partitioner` is used to output events to one or more channels
///
/// A: The Event to be sent
/// B: Source Component required for the tell method
pub trait Partitioner<A, B>
where
    A: 'static + Serialize + Send + Sync + Copy + Hash,
    B: ComponentDefinition + Sized + 'static,
{
    fn output(&mut self, event: A, source: &B, key: Option<u64>) -> crate::error::Result<()>;
    fn add_channel(&mut self, channel: Channel);
    fn remove_channel(&mut self, channel: Channel);
}

/// `channel_output` takes an event and sends it to another
/// component. Either locally (ActorRef) or remote (ActorPath)
fn channel_output<A, B>(
    channel: &Channel,
    event: A,
    source: &B,
    key: Option<u64>,
) -> crate::error::Result<()>
where
    A: 'static + Serialize + Send + Sync + Copy + Hash,
    B: ComponentDefinition + Sized + 'static,
{
    match &channel {
        Channel::Local(actor_ref) => {
            actor_ref.tell(Box::new(event), source);
        }
        Channel::Remote(actor_path) => {
            let serialised_event: Vec<u8> = bincode::serialize(&event)
                .map_err(|e| Error::new(SerializationError(e.to_string())))?;

            // TODO: Handle Timestamps...
            if let Some(key) = key {
                let keyed_msg = create_keyed_element(serialised_event, 1, key);
                actor_path.tell(keyed_msg, source);
            } else {
                let element_msg = create_element(serialised_event, 1);
                actor_path.tell(element_msg, source);
            }
        }
    }
    Ok(())
}
