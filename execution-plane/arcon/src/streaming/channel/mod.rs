use crate::data::{ArconMessage, ArconType};
use kompact::prelude::{ActorPath, ActorRefStrong};

pub mod strategy;

#[derive(Clone)]
pub enum Channel<A: 'static + ArconType> {
    Local(ActorRefStrong<ArconMessage<A>>),
    Remote(ActorPath),
}
