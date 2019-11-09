use crate::data::{ArconMessage, ArconType};
use kompact::prelude::{ActorPath, ActorRefStrong};

pub mod strategy;

#[derive(Clone)]
pub enum Channel<A: ArconType> {
    Local(ActorRefStrong<ArconMessage<A>>),
    Remote(ActorPath),
}
