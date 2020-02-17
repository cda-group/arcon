// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

/// Available Channel Strategies
pub mod strategy;

use crate::data::{flight_serde::FlightSerde, ArconMessage, ArconType};
use kompact::prelude::{ActorPath, ActorRefStrong};

/// A Channel represents a connection to another Component
#[derive(Clone)]
pub enum Channel<A: ArconType> {
    /// A typed local queue
    Local(ActorRefStrong<ArconMessage<A>>),
    /// Remote based queue containing a remote ActorPath identifier and an Arcon Serialiser
    Remote((ActorPath, FlightSerde)),
}
