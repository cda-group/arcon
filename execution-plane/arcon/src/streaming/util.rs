use crate::prelude::{ArconType, Mute};

pub fn mute_strategy<A: ArconType>() -> Box<Mute<A>> {
    Box::new(Mute::new())
}
