// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::data::serde::{reliable_remote::ReliableSerde, unsafe_remote::UnsafeSerde};
use crate::data::{ArconEvent, ArconMessage, ArconType};
use crate::prelude::KompactSystem;
use crate::stream::channel::{ArconSerde, Channel};

pub mod broadcast;
pub mod forward;
pub mod key_by;
pub mod mute;
pub mod round_robin;

/// `ChannelStrategy` manages batching and sending of message downstream
pub trait ChannelStrategy<A>: Send
where
    A: ArconType,
{
    /// Add event to outgoing buffer
    fn add(&mut self, event: ArconEvent<A>);
    /// Flush batch of events out
    fn flush(&mut self, source: &KompactSystem);
    /// Add event and flush directly
    fn add_and_flush(&mut self, event: ArconEvent<A>, source: &KompactSystem);
    /// Dynamically add channel
    fn add_channel(&mut self, _: Channel<A>) {
        unimplemented!();
    }
    /// Dynamically remove channel
    fn remove_channel(&mut self, _: Channel<A>) {
        unimplemented!();
    }
}

/// `send` pushes an ArconMessage onto a Component queue
///
/// The message may be sent to a local or remote component
#[inline]
fn send<A>(channel: &Channel<A>, message: ArconMessage<A>, source: &KompactSystem)
where
    A: ArconType,
{
    match channel {
        Channel::Local(actor_ref) => {
            actor_ref.tell(message);
        }
        Channel::Remote((actor_path, arcon_serde)) => match &arcon_serde {
            ArconSerde::Unsafe => {
                let unsafe_msg = UnsafeSerde(message);
                actor_path.tell(unsafe_msg, source);
            }
            ArconSerde::Reliable => {
                let reliable_msg = ReliableSerde(message);
                actor_path.tell(reliable_msg, source);
            }
        },
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[key_by(id)]
    #[arcon]
    #[derive(prost::Message)]
    pub struct Input {
        #[prost(uint32, tag = "1")]
        pub id: u32,
    }
}
