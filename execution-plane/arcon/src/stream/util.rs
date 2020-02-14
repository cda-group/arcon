// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::prelude::{ArconType, ChannelStrategy, Mute};

pub fn mute_strategy<A: ArconType>() -> ChannelStrategy<A> {
    ChannelStrategy::Mute(Mute::new())
}
