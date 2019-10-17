use crate::prelude::*;
use core::marker::PhantomData;

// Output strategy which does nothing, add to a node to mute it
pub struct Mute<A>
where
    A: 'static + ArconType,
{
    phantom_a: PhantomData<A>,
}

impl<A> Mute<A>
where
    A: 'static + ArconType,
{
    pub fn new() -> Mute<A> {
        Mute {
            phantom_a: PhantomData,
        }
    }
}

impl<A> ChannelStrategy<A> for Mute<A>
where
    A: 'static + ArconType,
{
    fn output(&mut self, _message: ArconMessage<A>, _source: &KompactSystem) -> ArconResult<()> {
        Ok(())
    }
    fn add_channel(&mut self, _channel: Channel<A>) {}
    fn remove_channel(&mut self, _channel: Channel<A>) {}
}
