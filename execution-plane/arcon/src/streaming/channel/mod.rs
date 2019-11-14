pub mod strategy;

use crate::data::{serde::ArconSerde, ArconMessage, ArconType};
use kompact::prelude::{ActorPath, ActorRefStrong};

#[derive(Clone)]
pub enum Channel<A: ArconType> {
    Local(ActorRefStrong<ArconMessage<A>>),
    Remote((ActorPath, ArconSerde<A>)),
}
