use crate::data::{reliable_remote::ReliableSerde, unsafe_remote::UnsafeSerde};
use crate::data::{ArconMessage, ArconType};
use kompact::prelude::{ActorPath, ActorRefStrong};
pub mod strategy;

#[derive(Clone)]
pub enum ArconSerde<A: ArconType> {
    Unsafe(UnsafeSerde<A>),
    Reliable(ReliableSerde<A>),
}

#[derive(Clone)]
pub enum Channel<A: ArconType> {
    Local(ActorRefStrong<ArconMessage<A>>),
    Remote((ActorPath, ArconSerde<A>)),
}
