// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::prelude::{ArconType, Mute};

pub fn mute_strategy<A: ArconType>() -> Box<Mute<A>> {
    Box::new(Mute::new())
}
