// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::nexmark::Bid;
use arcon::prelude::*;

/// ConcurrencyConversion
/// Convert bid price from U.S dollars to Euros
pub fn q1() {
    #[inline(always)]
    fn map_fn(bid: &mut Bid) {
        bid.price = (bid.price * 89) / 100;
    }

    // TODO
    let channel_strategy = ChannelStrategy::Mute;
    let _node = Node::<Bid, Bid>::new(
        1.into(),
        vec![2.into()],
        channel_strategy,
        Box::new(MapInPlace::new(&map_fn)),
        Box::new(InMemory::new("perf").unwrap()),
    );
}
