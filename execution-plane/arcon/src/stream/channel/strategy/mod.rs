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

/// A `ChannelStrategy` defines a strategy of how messages are sent downstream
///
/// Common strategies include (one-to-one)[Forward] and (one-to-many)[Broadcast]
pub enum ChannelStrategy<A>
where
    A: ArconType,
{
    Forward(forward::Forward<A>),
    Broadcast(broadcast::Broadcast<A>),
    KeyBy(key_by::KeyBy<A>),
    RoundRobin(round_robin::RoundRobin<A>),
    Mute(mute::Mute<A>),
}

impl<A> ChannelStrategy<A>
where
    A: ArconType,
{
    /// Add event to outgoing buffer
    #[inline]
    pub fn add(&mut self, event: ArconEvent<A>) {
        match self {
            ChannelStrategy::Forward(s) => s.add(event),
            ChannelStrategy::Broadcast(s) => s.add(event),
            ChannelStrategy::KeyBy(s) => s.add(event),
            ChannelStrategy::RoundRobin(s) => s.add(event),
            ChannelStrategy::Mute(_) => (),
        }
    }
    /// Add event and flush directly
    #[inline]
    pub fn add_and_flush(&mut self, event: ArconEvent<A>, source: &KompactSystem) {
        match self {
            ChannelStrategy::Forward(s) => s.add_and_flush(event, source),
            ChannelStrategy::Broadcast(s) => s.add_and_flush(event, source),
            ChannelStrategy::KeyBy(s) => s.add_and_flush(event, source),
            ChannelStrategy::RoundRobin(s) => s.add_and_flush(event, source),
            ChannelStrategy::Mute(_) => (),
        }
    }
    /// Flush batch of events out
    #[inline]
    pub fn flush(&mut self, source: &KompactSystem) {
        match self {
            ChannelStrategy::Forward(s) => s.flush(source),
            ChannelStrategy::Broadcast(s) => s.flush(source),
            ChannelStrategy::KeyBy(s) => s.flush(source),
            ChannelStrategy::RoundRobin(s) => s.flush(source),
            ChannelStrategy::Mute(_) => (),
        }
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

    #[arcon_keyed(id)]
    #[derive(prost::Message)]
    pub struct Input {
        #[prost(uint32, tag = "1")]
        pub id: u32,
    }
}
