// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

pub mod event_timer;
#[cfg(feature = "socket")]
pub mod io;

use std::time::{SystemTime, UNIX_EPOCH};

pub fn get_system_time() -> u64 {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");

    since_the_epoch.as_millis() as u64
}

pub trait SafelySendableFn<Args>: Fn<Args> + Send + Sync {}
impl<Args, F> SafelySendableFn<Args> for F where F: Fn<Args> + Send + Sync {}
