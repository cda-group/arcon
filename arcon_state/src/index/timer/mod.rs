use super::{hash::HashIndex, value::ValueIndex, IndexOps};
use crate::{
    backend::{handles::ActiveHandle, Backend, MapState, ValueState},
    data::{Key, Value},
    error::Result,
};
use hierarchical_hash_wheel_timer::{
    wheels::{quad_wheel::*, *},
    *,
};

use core::time::Duration;
use std::{cmp::Eq, hash::Hash};

#[derive(prost::Message, PartialEq, Clone)]
pub struct EventTimerEvent<E: Value> {
    #[prost(uint64, tag = "1")]
    time_when_scheduled: u64,
    #[prost(uint64, tag = "2")]
    timeout_millis: u64,
    #[prost(message, required, tag = "3")]
    payload: E,
}

impl<E: Value> EventTimerEvent<E> {
    fn new(time_when_scheduled: u64, timeout_millis: u64, payload: E) -> Self {
        EventTimerEvent {
            time_when_scheduled,
            timeout_millis,
            payload,
        }
    }
}

/// An Index for Stream Timers
///
/// The Index utilises the [QuadWheelWithOverflow] data structure
/// in order to manage the timers. The remaining state is kept in
/// other indexes such as HashIndex/ValueIndex.
pub struct TimerIndex<K, V, B>
where
    K: Key + Eq + Hash,
    V: Value,
    B: Backend,
{
    timer: QuadWheelWithOverflow<K>,
    timeouts: HashIndex<K, EventTimerEvent<V>, B>,
    current_time: ValueIndex<u64, B>,
}

impl<K, V, B> TimerIndex<K, V, B>
where
    K: Key + Eq + Hash,
    V: Value,
    B: Backend,
{
    pub fn new(
        timeouts_handle: ActiveHandle<B, MapState<K, EventTimerEvent<V>>>,
        time_handle: ActiveHandle<B, ValueState<u64>>,
    ) -> Self {
        Self {
            timer: QuadWheelWithOverflow::default(),
            timeouts: HashIndex::new(timeouts_handle),
            current_time: ValueIndex::new(time_handle),
        }
    }

    fn _replay_events(&mut self) {
        //let time = self.current_time.get().unwrap_or(&0);

        // TODO: support full iter at HashIndex
        /*
        for res in self
            .timeouts
            .iter()
            .expect("could not get timeouts")
        {
            let (id, entry) = res.expect("could not get timeout entry");
            let delay = entry.time_when_scheduled + entry.timeout_millis - time;
            if let Err(f) = self
                .timer
                .insert_with_delay(id, Duration::from_millis(delay))
            {
                panic!("A timeout has expired during replay: {:?}", f);
            }
        }
        */
    }

    #[inline(always)]
    pub fn set_time(&mut self, ts: u64) {
        self.current_time.put(ts);
    }

    #[inline(always)]
    pub fn current_time(&self) -> u64 {
        *self.current_time.get().unwrap_or(&0)
    }

    #[inline(always)]
    pub fn add_time(&mut self, by: u64) {
        let curr = self.current_time.get().unwrap();
        let new_time = curr + by;
        self.current_time.put(new_time);
    }

    #[inline(always)]
    pub fn tick_and_collect(&mut self, mut time_left: u32, res: &mut Vec<V>) {
        while time_left > 0 {
            match self.timer.can_skip() {
                Skip::Empty => {
                    // Timer is empty, no point in ticking it
                    self.add_time(time_left as u64);
                    return;
                }
                Skip::Millis(skip_ms) => {
                    // Skip forward
                    if skip_ms >= time_left {
                        // No more ops to gather, skip the remaining time_left and return
                        self.timer.skip(time_left);
                        self.add_time(time_left as u64);
                        return;
                    } else {
                        // Skip lower than time-left:
                        self.timer.skip(skip_ms);
                        self.add_time(skip_ms as u64);
                        time_left -= skip_ms;
                    }
                }
                Skip::None => {
                    for e in self.timer.tick() {
                        if let Some(entry) = self.take_entry(e) {
                            res.push(entry);
                        }
                    }
                    self.add_time(1u64);
                    time_left -= 1u32;
                }
            }
        }
    }

    // Lookup id, remove from storage, and return Executable action
    #[inline(always)]
    fn take_entry(&mut self, id: K) -> Option<V> {
        self.timeouts
            .remove(&id)
            .expect("no timeout found for id") // this wouldn't necessarily be an error anymore if we add a cancellation API at some point
            .map(|e| e.payload)
    }

    #[inline(always)]
    pub fn schedule_after(&mut self, id: K, delay: u64, entry: V) -> Result<(), V> {
        match self
            .timer
            .insert_with_delay(id.clone(), Duration::from_millis(delay))
        {
            Ok(_) => {
                // TODO: fix map_err
                let event = EventTimerEvent::new(self.current_time(), delay, entry);
                let _ = self.timeouts.put(id, event);
                Ok(())
            }
            Err(TimerError::Expired(_)) => Err(entry),
            Err(f) => panic!("Could not insert timer entry! {:?}", f),
        }
    }

    #[inline]
    pub fn schedule_at(&mut self, id: K, time: u64, entry: V) -> Result<(), V> {
        let curr_time = self.current_time.get().unwrap();
        // Check for expired target time
        if time <= *curr_time {
            Err(entry)
        } else {
            let delay = time - *curr_time;
            self.schedule_after(id, delay, entry)
        }
    }

    #[inline]
    pub fn advance_to(&mut self, ts: u64) -> Vec<V> {
        let mut res = Vec::new();
        let curr_time = self.current_time.get().unwrap();
        if ts < *curr_time {
            // advance_to called with lower timestamp than current time
            return res;
        }

        let mut time_left = ts - curr_time;
        while time_left > std::u32::MAX as u64 {
            self.tick_and_collect(std::u32::MAX, &mut res);
            time_left -= std::u32::MAX as u64;
        }
        // this cast must be safe now
        self.tick_and_collect(time_left as u32, &mut res);
        res
    }
}

impl<K, V, B> IndexOps for TimerIndex<K, V, B>
where
    K: Key + Eq + Hash,
    V: Value,
    B: Backend,
{
    fn persist(&mut self) -> crate::error::Result<()> {
        self.timeouts.persist()?;
        self.current_time.persist()?;
        Ok(())
    }
}

/*
#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::sled::Sled;

    #[test]
    fn timer_index_test() {
        let backend = Sled::create(&std::path::Path::new("/tmp/")).unwrap();
        let capacity = 128;
        let mut index: TimerIndex<u64, u64, Sled> =
            TimerIndex::new(capacity, std::sync::Arc::new(backend));
        // Timer per key...
        index.schedule_at(1, 1000, 10).unwrap();
        index.schedule_at(2, 1600, 10).unwrap();
        let evs = index.advance_to(1500);
        assert_eq!(evs.len(), 1);
        let evs = index.advance_to(2000);
        assert_eq!(evs.len(), 1);
    }
}
*/
