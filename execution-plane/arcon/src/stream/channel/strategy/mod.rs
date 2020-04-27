// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{
        flight_serde::{reliable_remote::ReliableSerde, unsafe_remote::UnsafeSerde, FlightSerde},
        ArconEvent, ArconMessage, ArconType,
    },
    stream::channel::Channel,
};

pub mod broadcast;
pub mod forward;
pub mod key_by;
pub mod round_robin;

/// A `ChannelStrategy` defines a strategy of how messages are sent downstream
///
/// Common strategies include (one-to-one)[forward::Forward] and (one-to-many)[broadcast::Broadcast]
pub enum ChannelStrategy<A>
where
    A: ArconType,
{
    /// Send messages to a single Component
    Forward(forward::Forward<A>),
    /// Broadcasts the message to a Vec of `Channels`
    Broadcast(broadcast::Broadcast<A>),
    /// Partition data to a set of `Channels` based on keyed hash
    KeyBy(key_by::KeyBy<A>),
    /// Send messages to a Vec of `Channels` in a Round Robin fashion
    RoundRobin(round_robin::RoundRobin<A>),
    /// A strategy that simply does nothing
    Mute,
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
            ChannelStrategy::Mute => (),
        }
    }
    /// Flush batch of events out
    #[inline]
    pub fn flush(&mut self) {
        match self {
            ChannelStrategy::Forward(s) => s.flush(),
            ChannelStrategy::Broadcast(s) => s.flush(),
            ChannelStrategy::KeyBy(s) => s.flush(),
            ChannelStrategy::RoundRobin(s) => s.flush(),
            ChannelStrategy::Mute => (),
        }
    }
    /// Returns number of outgoing channels
    #[inline]
    pub(crate) fn num_channels(&self) -> usize {
        match self {
            ChannelStrategy::Forward(_) => 1,
            ChannelStrategy::Broadcast(s) => s.num_channels(),
            ChannelStrategy::KeyBy(s) => s.num_channels(),
            ChannelStrategy::RoundRobin(s) => s.num_channels(),
            ChannelStrategy::Mute => 0,
        }
    }
}

/// `send` pushes an ArconMessage onto a Component queue
///
/// The message may be sent to a local or remote component
#[inline]
fn send<A>(channel: &Channel<A>, message: ArconMessage<A>)
where
    A: ArconType,
{
    match channel {
        Channel::Local(actor_ref) => {
            actor_ref.tell(message);
        }
        Channel::Remote(actor_path, FlightSerde::Unsafe, dispatcher_source) => {
            let unsafe_msg = UnsafeSerde(message.into());
            actor_path.tell(unsafe_msg, dispatcher_source);
        }
        Channel::Remote(actor_path, FlightSerde::Reliable, dispatcher_source) => {
            let reliable_msg = ReliableSerde(message.into());
            actor_path.tell(reliable_msg, dispatcher_source);
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[arcon_keyed(id)]
    pub struct Input {
        #[prost(uint32, tag = "1")]
        pub id: u32,
    }
}
