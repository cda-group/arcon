use crate::error::timer::{TimerExpiredError, TimerResult};
use crate::index::hash_table::eager::EagerHashTable;
use arcon_state::{
    backend::{
        handles::{ActiveHandle, Handle},
        Backend, ValueState,
    },
    data::Value,
    error::Result,
};
use core::time::Duration;
use hierarchical_hash_wheel_timer::{
    wheels::{quad_wheel::*, *},
    *,
};
use std::sync::Arc;

#[derive(prost::Message, PartialEq, Clone)]
pub struct TimerEvent<E: Value> {
    #[prost(uint64, tag = "1")]
    time_when_scheduled: u64,
    #[prost(uint64, tag = "2")]
    timeout_millis: u64,
    #[prost(message, required, tag = "3")]
    payload: E,
}

impl<E: Value> TimerEvent<E> {
    fn new(time_when_scheduled: u64, timeout_millis: u64, payload: E) -> Self {
        TimerEvent {
            time_when_scheduled,
            timeout_millis,
            payload,
        }
    }
}

#[derive(prost::Message, PartialEq, Clone, Copy)]
pub struct TimeoutId {
    #[prost(uint64, tag = "1")]
    key: u64,
    #[prost(uint64, tag = "2")]
    timestamp: u64,
}

pub struct TimerEntry<V> {
    id: TimeoutId,
    value: V,
}
impl<V> TimerEntry<V> {
    #[inline]
    pub fn key(&self) -> u64 {
        self.id.key
    }
    #[inline]
    pub fn value(self) -> V {
        self.value
    }
}

pub trait ArconTimer: Send {
    type Value: std::fmt::Debug;

    fn schedule_at(&mut self, time: u64, entry: Self::Value) -> TimerResult<Self::Value>;
    fn advance_to(&mut self, ts: u64) -> Result<Vec<TimerEntry<Self::Value>>>;
    fn get_time(&self) -> Result<u64>;
    fn active_key(&mut self, key: u64);
}

/// An Index for Stream Timers
///
/// The Index utilises the [QuadWheelWithOverflow] data structure
/// in order to manage the timers. The remaining state is kept in
/// other indexes such as Map/Value.
pub struct Timer<V, B>
where
    V: Value,
    B: Backend,
{
    timer: QuadWheelWithOverflow<TimeoutId>,
    current_key: u64,
    timeouts: EagerHashTable<TimeoutId, TimerEvent<V>, B>,
    time_handle: ActiveHandle<B, ValueState<u64>>,
}

impl<V, B> Timer<V, B>
where
    V: Value,
    B: Backend,
{
    pub fn new(id: impl Into<String>, backend: Arc<B>) -> Self {
        let id = id.into();
        let timeouts_id = format!("_{}_timeouts", id);
        let time_id = format!("_{}_time", id);

        let mut handle = Handle::value(time_id);
        backend.register_value_handle(&mut handle);

        let time_handle = handle.activate(backend.clone());

        let mut timer = Self {
            timer: QuadWheelWithOverflow::default(),
            current_key: 0,
            timeouts: EagerHashTable::new(timeouts_id, backend),
            time_handle,
        };

        // replay and insert back if any exists
        timer.replay_events();

        timer
    }

    fn replay_events(&mut self) {
        let time = self.current_time().unwrap();

        for res in self.timeouts.iter().expect("could not get timeouts") {
            let (id, entry) = res.expect("could not get timeout entry");
            let delay = entry.time_when_scheduled + entry.timeout_millis - time;
            if let Err(f) = self
                .timer
                .insert_with_delay(id, Duration::from_millis(delay))
            {
                panic!("A timeout has expired during replay: {:?}", f);
            }
        }
    }

    #[inline(always)]
    pub fn set_time(&mut self, ts: u64) -> Result<()> {
        self.time_handle.fast_set(ts)
    }

    #[inline(always)]
    pub fn current_time(&self) -> Result<u64> {
        let time = self.time_handle.get()?;
        Ok(time.unwrap_or(0))
    }

    #[inline(always)]
    pub fn add_time(&mut self, by: u64) -> Result<()> {
        let curr = self.current_time().unwrap();
        let new_time = curr + by;
        self.set_time(new_time)
    }

    #[inline(always)]
    pub fn tick_and_collect(
        &mut self,
        mut time_left: u32,
        res: &mut Vec<TimerEntry<V>>,
    ) -> Result<()> {
        while time_left > 0 {
            match self.timer.can_skip() {
                Skip::Empty => {
                    // Timer is empty, no point in ticking it
                    self.add_time(time_left as u64)?;
                    return Ok(());
                }
                Skip::Millis(skip_ms) => {
                    // Skip forward
                    if skip_ms >= time_left {
                        // No more ops to gather, skip the remaining time_left and return
                        self.timer.skip(time_left);
                        self.add_time(time_left as u64)?;
                        return Ok(());
                    } else {
                        // Skip lower than time-left:
                        self.timer.skip(skip_ms);
                        self.add_time(skip_ms as u64)?;
                        time_left -= skip_ms;
                    }
                }
                Skip::None => {
                    for e in self.timer.tick() {
                        if let Some(entry) = self.take_entry(e) {
                            res.push(entry);
                        }
                    }
                    self.add_time(1u64)?;
                    time_left -= 1u32;
                }
            }
        }
        Ok(())
    }

    // Lookup id, remove from storage, and return Executable action
    #[inline(always)]
    fn take_entry(&mut self, id: TimeoutId) -> Option<TimerEntry<V>> {
        self.timeouts
            .remove(&id)
            .expect("no timeout found for id") // this wouldn't necessarily be an error anymore if we add a cancellation API at some point
            .map(|e| TimerEntry {
                id,
                value: e.payload,
            })
    }

    #[inline(always)]
    pub fn schedule_after(&mut self, id: TimeoutId, delay: u64, entry: V) -> TimerResult<V> {
        match self
            .timer
            .insert_with_delay(id, Duration::from_millis(delay))
        {
            Ok(_) => {
                let event = TimerEvent::new(self.current_time().unwrap(), delay, entry);
                self.timeouts.put(id, event)?;
                Ok(Ok(()))
            }
            Err(TimerError::Expired(_)) => Ok(Err(TimerExpiredError {
                current_time: self.current_time().unwrap(),
                scheduled_time: delay,
                entry,
            })),
            Err(f) => crate::reportable_error!("Could not insert timer entry! {:?}", f),
        }
    }
}

impl<V, B> ArconTimer for Timer<V, B>
where
    V: Value,
    B: Backend,
{
    type Value = V;

    fn active_key(&mut self, key: u64) {
        self.current_key = key;
    }

    #[inline]
    fn schedule_at(&mut self, time: u64, entry: Self::Value) -> TimerResult<Self::Value> {
        let curr_time = self.current_time().unwrap();
        let timeout_id = TimeoutId {
            key: self.current_key,
            timestamp: time,
        };
        // Check for expired target time
        if time <= curr_time {
            Ok(Err(TimerExpiredError {
                current_time: curr_time,
                scheduled_time: time,
                entry,
            }))
        } else {
            let delay = time - curr_time;
            self.schedule_after(timeout_id, delay, entry)
        }
    }

    #[inline]
    fn advance_to(&mut self, ts: u64) -> Result<Vec<TimerEntry<Self::Value>>> {
        let mut res = Vec::new();
        let curr_time = self.current_time().unwrap();
        if ts < curr_time {
            // advance_to called with lower timestamp than current time
            return Ok(res);
        }

        let mut time_left = ts - curr_time;
        while time_left > std::u32::MAX as u64 {
            self.tick_and_collect(std::u32::MAX, &mut res)?;
            time_left -= std::u32::MAX as u64;
        }
        // this cast must be safe now
        self.tick_and_collect(time_left as u32, &mut res)?;
        Ok(res)
    }
    fn get_time(&self) -> Result<u64> {
        let time = self.time_handle.get()?;
        Ok(time.unwrap_or(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::temp_backend;
    use arcon_state::Sled;
    use std::sync::Arc;

    #[test]
    fn timer_index_test() {
        let backend = Arc::new(temp_backend::<Sled>());
        let mut timer = Timer::new("mytimer", backend);

        // Timer per key...
        timer.active_key(1);
        let _ = timer.schedule_at(1000, 10).unwrap();
        timer.active_key(2);
        let _ = timer.schedule_at(1600, 10).unwrap();
        let evs = timer.advance_to(1500).unwrap();
        assert_eq!(evs.len(), 1);
        let evs = timer.advance_to(2000).unwrap();
        assert_eq!(evs.len(), 1);
    }
    // TODO: more elaborate tests
}
