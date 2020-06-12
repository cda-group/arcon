// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

#[cfg(feature = "socket")]
pub mod io;

pub mod prost_helpers;
pub mod system_killer;

use std::time::{SystemTime, UNIX_EPOCH};

#[inline]
pub fn get_system_time() -> u64 {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");

    since_the_epoch.as_millis() as u64
}

#[inline]
pub fn get_system_time_nano() -> u64 {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");

    since_the_epoch.as_nanos() as u64
}

pub trait SafelySendableFn<Args>: Fn<Args> + Send + Sync {}
impl<Args, F> SafelySendableFn<Args> for F where F: Fn<Args> + Send + Sync {}
