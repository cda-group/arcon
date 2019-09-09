use crate::data::*;
use crate::error::*;
use crate::streaming::channel::Channel;
use kompact::KompactSystem;

pub mod broadcast;
pub mod forward;
pub mod key_by;
pub mod round_robin;
pub mod shuffle;
pub mod mute;

/// `ChannelStrategy` is used to output events to one or more channels
///
/// A: The Event to be sent
/// B: Source Component required for the tell method
pub trait ChannelStrategy<A>: Send + Sync
where
    A: 'static + ArconType,
{
    fn output(&mut self, event: ArconMessage<A>, source: &KompactSystem) -> ArconResult<()>;
    fn add_channel(&mut self, channel: Channel);
    fn remove_channel(&mut self, channel: Channel);
}

/// `channel_output` takes an event and sends it to another component.
/// Either locally through an ActorRef, or remote (ActorPath)
fn channel_output<A>(
    channel: &Channel,
    message: ArconMessage<A>,
    source: &KompactSystem,
) -> ArconResult<()>
where
    A: 'static + ArconType,
{
    match channel {
        Channel::Local(actor_ref) => {
            actor_ref.tell(Box::new(message), source);
        }
        Channel::Remote(actor_path) => {
            actor_path.tell(message.to_remote()?, source);
        }
    }
    Ok(())
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[key_by(id)]
    #[arcon]
    pub struct Input {
        pub id: u32,
    }
}
