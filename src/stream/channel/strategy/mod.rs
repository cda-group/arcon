// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{
        flight_serde::{reliable_remote::ReliableSerde, unsafe_remote::UnsafeSerde, FlightSerde},
        ArconEvent, ArconMessage, ArconType,
    },
    stream::channel::Channel,
};
use kompact::prelude::{ComponentDefinition, SerError};

pub mod broadcast;
pub mod forward;
pub mod key_by;
pub mod round_robin;

use crate::dataflow::stream::ChannelTrait;

impl<A: ArconType> ChannelTrait for ChannelStrategy<A> {}

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
    pub fn add(&mut self, event: ArconEvent<A>, source: &impl ComponentDefinition) {
        match self {
            ChannelStrategy::Forward(s) => s.add(event, source),
            ChannelStrategy::Broadcast(s) => s.add(event, source),
            ChannelStrategy::KeyBy(s) => s.add(event, source),
            ChannelStrategy::RoundRobin(s) => s.add(event, source),
            ChannelStrategy::Mute => (),
        }
    }
    /// Flush batch of events out
    #[inline]
    pub fn flush(&mut self, source: &impl ComponentDefinition) {
        match self {
            ChannelStrategy::Forward(s) => s.flush(source),
            ChannelStrategy::Broadcast(s) => s.flush(source),
            ChannelStrategy::KeyBy(s) => s.flush(source),
            ChannelStrategy::RoundRobin(s) => s.flush(source),
            ChannelStrategy::Mute => (),
        }
    }
    /// Returns number of outgoing channels
    #[inline]
    pub fn num_channels(&self) -> usize {
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
fn send<A: ArconType>(
    channel: &Channel<A>,
    message: ArconMessage<A>,
    source: &impl ComponentDefinition,
) -> Result<(), SerError> {
    match channel {
        Channel::Local(actor_ref) => {
            actor_ref.tell(message);
            Ok(())
        }
        Channel::Remote(actor_path, FlightSerde::Unsafe) => {
            let unsafe_msg = UnsafeSerde(message.into());
            actor_path.tell_serialised(unsafe_msg, source)
        }
        Channel::Remote(actor_path, FlightSerde::Reliable) => {
            let reliable_msg = ReliableSerde(message.into());
            actor_path.tell_serialised(reliable_msg, source)
        }
    }
}

#[cfg(test)]
pub mod tests {
    #[cfg_attr(feature = "arcon_serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Arcon, prost::Message, Clone, abomonation_derive::Abomonation)]
    #[arcon(unsafe_ser_id = 12, reliable_ser_id = 13, version = 1)]
    pub struct Input {
        #[prost(uint32, tag = "1")]
        pub id: u32,
    }
}
