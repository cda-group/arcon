use crate::streaming::Channel;
use kompact::{ActorPath, ActorRef};
use std::hash::Hash;
use std::sync::Arc;

pub mod broadcast;
pub mod forward;
pub mod hash;

pub trait Partitioner<A: 'static + Send + Sync + Copy + Hash> {
    fn output(&mut self, event: A, source: &Channel, key: Option<u64>) -> crate::error::Result<()>;
    fn add_channel(&mut self, channel: Channel);
    fn remove_channel(&mut self, channel: Channel);
}
