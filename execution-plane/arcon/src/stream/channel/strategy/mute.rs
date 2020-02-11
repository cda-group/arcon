// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::prelude::*;
use core::marker::PhantomData;

// Output strategy which does nothing, add to a node to mute it
pub struct Mute<A>
where
    A: 'static + ArconType,
{
    phantom_a: PhantomData<A>,
}

impl<A> Mute<A>
where
    A: 'static + ArconType,
{
    pub fn new() -> Mute<A> {
        Mute {
            phantom_a: PhantomData,
        }
    }
}

impl<A> Default for Mute<A>
where
    A: 'static + ArconType,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<A> ChannelStrategy<A> for Mute<A>
where
    A: 'static + ArconType,
{
    fn add(&mut self, _: ArconEvent<A>) {
        // do nothing..
    }
    fn flush(&mut self, _: &KompactSystem) {}

    fn add_and_flush(&mut self, _: ArconEvent<A>, _: &KompactSystem) {}
}
