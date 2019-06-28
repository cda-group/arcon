use crate::prelude::Serialize;
use crate::streaming::Channel;
use kompact::{ActorPath, ActorRef, ComponentDefinition};
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
