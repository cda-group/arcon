use kompact::prelude::{ActorPath, ActorRef};
use crate::data::{ArconMessage, ArconType};

pub mod strategy;

#[derive(Clone)]
pub enum Channel<A: 'static + ArconType> {
    Local(ActorRef<ArconMessage<A>>),
    Remote(ActorPath),
}
