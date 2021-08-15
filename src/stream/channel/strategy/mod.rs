// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

#[cfg(feature = "unsafe_flight")]
use crate::data::flight_serde::unsafe_remote::UnsafeSerde;
use crate::{
    data::{
        flight_serde::{reliable_remote::ReliableSerde, FlightSerde},
        ArconEvent, ArconMessage, ArconType,
    },
    stream::channel::Channel,
};
use kompact::prelude::{ComponentDefinition, SerError};
use std::sync::Arc;

#[allow(dead_code)]
pub mod broadcast;
pub mod forward;
pub mod keyed;

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
    #[allow(dead_code)]
    Broadcast(broadcast::Broadcast<A>),
    /// Partition data to a set of `Channels` based on keyed hash
    Keyed(keyed::Keyed<A>),
    /// A strategy that prints to the console
    Console,
    /// A strategy that simply does nothing
    Mute,
}

impl<A> ChannelStrategy<A>
where
    A: ArconType,
{
    /// Add event to outgoing buffer
    ///
    /// The function returns a tuple of channel and msg if the buffers are full or a marker was inputted.
    #[inline]
    pub fn push(
        &mut self,
        event: ArconEvent<A>,
    ) -> impl IntoIterator<Item = (Arc<Channel<A>>, ArconMessage<A>)> {
        match self {
            ChannelStrategy::Forward(s) => s.add(event),
            ChannelStrategy::Keyed(s) => s.add(event),
            ChannelStrategy::Broadcast(s) => s.add(event),
            ChannelStrategy::Console => {
                println!("{:?}", event);
                Vec::new()
            }
            ChannelStrategy::Mute => Vec::new(),
        }
    }

    /// Returns number of outgoing channels
    #[inline]
    #[allow(dead_code)]
    pub fn num_channels(&self) -> usize {
        match self {
            ChannelStrategy::Forward(_) => 1,
            ChannelStrategy::Broadcast(s) => s.num_channels(),
            ChannelStrategy::Keyed(s) => s.num_channels(),
            ChannelStrategy::Console => 0,
            ChannelStrategy::Mute => 0,
        }
    }
}

/// `send` pushes an ArconMessage onto a Component queue
///
/// The message may be sent to a local or remote component
#[inline]
pub(crate) fn send<A: ArconType>(
    channel: &Channel<A>,
    message: ArconMessage<A>,
    source: &impl ComponentDefinition,
) -> Result<(), SerError> {
    match channel {
        Channel::Local(actor_ref) => {
            actor_ref.tell(message);
            Ok(())
        }
        #[cfg(feature = "unsafe_flight")]
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
    #[cfg(feature = "unsafe_flight")]
    use abomonation_derive::*;

    #[cfg_attr(feature = "arcon_serde", derive(serde::Serialize, serde::Deserialize))]
    #[cfg_attr(feature = "unsafe_flight", derive(Abomonation))]
    #[derive(Arcon, prost::Message, Clone)]
    #[arcon(unsafe_ser_id = 12, reliable_ser_id = 13, version = 1)]
    pub struct Input {
        #[prost(uint32, tag = "1")]
        pub id: u32,
    }
}
