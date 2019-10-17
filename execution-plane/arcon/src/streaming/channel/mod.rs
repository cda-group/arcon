use crate::data::{ArconMessage, ArconType};
use kompact::prelude::{ActorPath, ActorRef};

pub mod strategy;

#[derive(Clone)]
pub enum Channel<A: 'static + ArconType> {
    Local(ActorRef<ArconMessage<A>>),
    Remote(ActorPath),
}
