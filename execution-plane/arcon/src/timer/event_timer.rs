// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use core::time::Duration;
use hierarchical_hash_wheel_timer::{
    wheels::{quad_wheel::*, *},
    *,
};
use prost::Message;
#[cfg(feature = "arcon_serde")]
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt, fmt::Debug};
use uuid::Uuid;
/*
    EventTimer: Abstraction of timer with underlying QuadWheel scheduling
        Has no relation to system time and does not use any threads.
        Useful for guaranteed sequentiality while still allowing scheduling semantics.

    EventTimer.set_time(timestamp) sets the timers time to timestamp, must be called before
        schedule at to function properly.
    EventTimer.schedule_at(timestamp, Function) schedules a function to "occur" at timestamp
    EventTimer.advance_to(timestamp) returns the ordered set of functions that are scheduled
        between now and timestamp, skipping forward efficiently

    The set_time, schedule_at and advance_to uses u64 timestamps as UNIX timestamps in Seconds
    schedule_once, schedule_periodic and the underlying QuadWheel allows usage of milliseconds.

    QuadWheel stores placeholder functions while the handles HashMap stores the real functions
    (this allows usage of self within scheduled functions while still utilizing the kompact wheels.rs)
    execute() method in this implementation is the translation between placeholder and real function.

    Usage is thus to store the timer within a component, schedule events on it and using
        advance_to to return ordered set of actions to perform.
*/

#[cfg_attr(feature = "arcon_serde", derive(Serialize, Deserialize))]
#[derive(Message, PartialEq, Clone)]
pub struct EventTimerEvent<E: Message + Default + PartialEq> {
    #[prost(uint64, tag = "1")]
    time_when_scheduled: u64,
    #[prost(uint64, tag = "2")]
    timeout_millis: u64,
    #[prost(message, required, tag = "3")]
    payload: E,
}

impl<E: Message + Default + PartialEq> EventTimerEvent<E> {
    fn new(time_when_scheduled: u64, timeout_millis: u64, payload: E) -> EventTimerEvent<E> {
        EventTimerEvent {
            time_when_scheduled,
            timeout_millis,
            payload,
        }
    }
}

/// The serializable part of EventTimer<E>
#[cfg_attr(feature = "arcon_serde", derive(Serialize, Deserialize))]
#[derive(Clone, Message)]
pub struct SerializableEventTimer<E>
where
    E: Message + Default + PartialEq,
{
    #[prost(uint64, tag = "1")]
    time: u64,
    // this was HashMap<Uuid, (u64, Duration, E)>, but then prost happened
    #[prost(map(string, message), tag = "2")]
    handles: HashMap<String, EventTimerEvent<E>>,
}

// when serializing this we treat the timer field as transient and recreate it from the serializable
// part when deserializing
pub struct EventTimer<E>
where
    E: Message + Default + PartialEq,
{
    // Since the backing store uses string keys, there's no point in storing Uuids here and converting back and forth
    timer: QuadWheelWithOverflow<String>,
    pub inner: SerializableEventTimer<E>,
}

impl<E> Default for EventTimer<E>
where
    E: Message + Default + PartialEq,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<E> EventTimer<E>
where
    E: Message + Default + PartialEq,
{
    pub fn new() -> EventTimer<E> {
        EventTimer {
            timer: QuadWheelWithOverflow::default(),
            inner: SerializableEventTimer {
                time: 0u64,
                handles: HashMap::new(),
            },
        }
    }

    // Should be called before scheduling anything
    #[inline(always)]
    pub fn set_time(&mut self, ts: u64) {
        self.inner.time = ts;
    }

    fn tick_and_collect(&mut self, mut time_left: u32, res: &mut Vec<E>) -> () {
        while time_left > 0 {
            match self.timer.can_skip() {
                Skip::Empty => {
                    // Timer is empty, no point in ticking it
                    self.inner.time += time_left as u64;
                    return;
                }
                Skip::Millis(skip_ms) => {
                    // Skip forward
                    if skip_ms >= time_left {
                        // No more ops to gather, skip the remaining time_left and return
                        self.timer.skip(time_left);
                        self.inner.time += time_left as u64;
                        return;
                    } else {
                        // Skip lower than time-left:
                        self.timer.skip(skip_ms);
                        self.inner.time += skip_ms as u64;
                        time_left -= skip_ms;
                    }
                }
                Skip::None => {
                    for e in self.timer.tick() {
                        if let Some(entry) = self.take_entry(e) {
                            res.push(entry);
                        }
                    }
                    self.inner.time += 1u64;
                    time_left -= 1u32;
                }
            }
        }
    }

    // Lookup id, remove from storage, and return Executable action
    #[inline(always)]
    fn take_entry(&mut self, id: String) -> Option<E> {
        self.inner.handles.remove(&id).map(|x| x.payload)
    }
}

impl<E> TimerBackend<E> for EventTimer<E>
where
    E: Message + Default + PartialEq,
{
    fn schedule_after(&mut self, delay: u64, entry: E) -> Result<(), E> {
        // this seems a bit silly, but it is A way to generate a unique string, I suppose^^
        let id = Uuid::new_v4().to_string();

        match self
            .timer
            .insert_with_delay(id.clone(), Duration::from_millis(delay))
        {
            Ok(_) => {
                self.inner
                    .handles
                    .insert(id, EventTimerEvent::new(self.inner.time, delay, entry));
                Ok(())
            }
            Err(TimerError::Expired(_)) => Err(entry),
            Err(f) => panic!("Could not insert timer entry! {:?}", f),
        }
    }
    fn schedule_at(&mut self, time: u64, entry: E) -> Result<(), E> {
        // Check for expired target time
        if time <= self.inner.time {
            Err(entry)
        } else {
            self.schedule_after(time - self.inner.time, entry)
        }
    }

    fn current_time(&self) -> u64 {
        self.inner.time
    }

    fn advance_to(&mut self, ts: u64) -> Vec<E> {
        let mut res = Vec::new();
        if ts < self.inner.time {
            eprintln!("advance_to called with lower timestamp than current time");
            return res;
        }

        let mut time_left = ts - self.inner.time;
        while time_left > std::u32::MAX as u64 {
            self.tick_and_collect(std::u32::MAX, &mut res);
            time_left -= std::u32::MAX as u64;
        }
        // this cast must be safe now
        self.tick_and_collect(time_left as u32, &mut res);
        res
    }
}

impl<E> Debug for EventTimer<E>
where
    E: Message + Default + PartialEq,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<EventTimer>")
    }
}

// Allows the EventTimer to be scheduled on different threads, but should never be used concurrently
//unsafe impl<E> Send for EventTimer<E> where E: Message + Default + PartialEq {}

impl<E> From<SerializableEventTimer<E>> for EventTimer<E>
where
    E: Message + Default + PartialEq,
{
    fn from(SerializableEventTimer { time, handles }: SerializableEventTimer<E>) -> Self {
        let mut res = EventTimer::new();
        res.set_time(time);

        // the internal ids WILL change
        for (
            _id,
            EventTimerEvent {
                time_when_scheduled,
                timeout_millis,
                payload,
            },
        ) in handles
        {
            let new_timeout = time_when_scheduled + timeout_millis - time;
            if let Err(e) = res.schedule_after(new_timeout, payload) {
                // I don't know if this can actually happen with the semantics of our store (i.e. torn writes between the timestamp and the collection)
                // but if it does, it's probably bad and we need to deal with it at some point. Silently dropping timeouts is not a good idea.
                panic!("A timeout has expired during replay: {:?}", e);
            }
        }

        res
    }
}
