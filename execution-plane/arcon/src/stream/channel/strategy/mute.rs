// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::prelude::*;
use core::marker::PhantomData;

// Output strategy which does nothing, add to a node to mute it
pub struct Mute<A>
where
    A: ArconType,
{
    phantom_a: PhantomData<A>,
}

impl<A> Mute<A>
where
    A: ArconType,
{
    pub fn new() -> Mute<A> {
        Mute {
            phantom_a: PhantomData,
        }
    }
}

impl<A> Default for Mute<A>
where
    A: ArconType,
{
    fn default() -> Self {
        Self::new()
    }
}
