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
