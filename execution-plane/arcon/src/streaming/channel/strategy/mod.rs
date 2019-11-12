use crate::data::*;
use crate::error::*;
use crate::prelude::KompactSystem;
use crate::streaming::channel::Channel;

pub mod broadcast;
pub mod forward;
pub mod key_by;
pub mod mute;
pub mod round_robin;
pub mod shuffle;

/// `ChannelStrategy` is used to output events to one or more channels
///
/// A: The Event to be sent
pub trait ChannelStrategy<A>: Send + Sync
where
    A: ArconType,
{
    fn output(&mut self, event: ArconMessage<A>, source: &KompactSystem) -> ArconResult<()>;
    fn add_channel(&mut self, channel: Channel<A>);
    fn remove_channel(&mut self, channel: Channel<A>);
}

/// `channel_output` takes an event and sends it to another component.
/// Either locally through an ActorRef, or remote (ActorPath)
fn channel_output<A>(
    channel: &Channel<A>,
    message: ArconMessage<A>,
    source: &KompactSystem,
) -> ArconResult<()>
where
    A: ArconType,
{
    match channel {
        Channel::Local(actor_ref) => {
            actor_ref.tell(message);
        }
        Channel::Remote(actor_path) => {
            use crate::data::reliable_remote::*;
            let ser = ReliableSerde::<A>::new();
            //use crate::data::unsafe_remote::*;
            //let ser = UnsafeSerde::<A>::new();
            actor_path.tell((message, ser), source);
        }
    }
    Ok(())
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[key_by(id)]
    #[arcon]
    #[derive(prost::Message)]
    pub struct Input {
        #[prost(uint32, tag="1")]
        pub id: u32,
    }
}
