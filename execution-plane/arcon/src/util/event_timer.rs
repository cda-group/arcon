// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use core::time::Duration;
use kompact::timer::*;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt;
use std::fmt::Debug;
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

pub struct EventTimer<E: Clone> {
    timer: QuadWheelWithOverflow,
    time: u64,
    handles: HashMap<Uuid, E>,
}

impl<E: Clone> Default for EventTimer<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: Clone> EventTimer<E> {
    pub fn new() -> EventTimer<E> {
        EventTimer {
            timer: QuadWheelWithOverflow::new(),
            time: 0u64,
            handles: HashMap::new(),
        }
    }
    // Basic scheduling function
    fn schedule_once(&mut self, timeout: Duration, entry: E) {
        let id = Uuid::new_v4();
        self.handles.insert(id, entry);

        let e = TimerEntry::OneShot {
            id,
            timeout,
            action: Box::new(move |_| {}),
        };
        match self.timer.insert(e) {
            Ok(_) => (), // ok
            Err(TimerError::Expired(e)) => {
                self.execute(e);
            }
            Err(f) => panic!("Could not insert timer entry! {:?}", f),
        }
    }
    // Schedule at a specific time in the future
    pub fn schedule_at(&mut self, time: u64, entry: E) {
        // Check for bad target time
        if time < self.time {
            eprintln!("tried to schedule event which has already happened");
        } else {
            self.schedule_once(Duration::from_millis(time - self.time), entry);
        }
    }
    // Should be called before scheduling anything
    #[inline(always)]
    pub fn set_time(&mut self, ts: u64) {
        self.time = ts;
    }
    pub fn get_time(&mut self) -> u64 {
        self.time
    }
    //
    #[inline(always)]
    pub fn advance_to(&mut self, ts: u64) -> Vec<E> {
        let mut vec = Vec::new();
        if ts < self.time {
            eprintln!("advance_to called with lower timestamp than current time");
            return vec;
        }

        // TODO: type conversion mess
        let mut time_left = ts - self.time;
        while time_left > 0 {
            if let Skip::Millis(skip_ms) = self.timer.can_skip() {
                // Skip forward
                if skip_ms >= time_left.try_into().unwrap() {
                    // No more ops to gather, skip the remaining time_left and return
                    self.timer.skip((time_left).try_into().unwrap());
                    self.time += time_left;
                    return vec;
                } else {
                    // Skip lower than time-left:
                    self.timer.skip(skip_ms);
                    self.time += skip_ms as u64;
                    time_left -= skip_ms as u64;
                }
            } else {
                // Can't skip
                let mut res = self.timer.tick();
                for e in res.drain(..) {
                    if let Some(entry) = self.execute(e) {
                        vec.push(entry)
                    }
                }
                self.time += 1;
                time_left -= 1;
            }
        }
        vec
    }
    // Takes TimerEntry, reschedules it if necessary and returns Executable actions
    #[inline(always)]
    fn execute(&mut self, e: TimerEntry) -> Option<E> {
        let id = e.id();
        let res = self.handles.remove(&id);

        // Reschedule the event
        if let Some(re_e) = e.execute() {
            match self.timer.insert(re_e) {
                Ok(_) => (), // great
                Err(TimerError::Expired(re_e)) => {
                    // This could happen if someone specifies 0ms period
                    eprintln!("TimerEntry could not be inserted properly: {:?}", re_e);
                }
                Err(f) => panic!("Could not insert timer entry! {:?}", f),
            }
        }
        res
    }
}

impl<E: Clone> Debug for EventTimer<E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<EventTimer>")
    }
}

// Allows the EventTimer to be scheduled on different threads, but should never be used concurrently
unsafe impl<E: Clone> Send for EventTimer<E> {}
//unsafe impl<E: Clone> Sync for EventTimer<E> {} // Q: do we need this timer to be sync?
/*
pub(crate) enum TimerHandle<C: ComponentDefinition> {
    OneShot {
        _id: Uuid, // not used atm
        action: Box<dyn FnOnce(&mut C, Uuid) + Send + 'static>,
    },
    Periodic {
        _id: Uuid, // not used atm
        action: Rc<Fn(&mut C, Uuid) + Send + 'static>,
    },
}

pub enum ExecuteAction<C: ComponentDefinition> {
    None,
    Periodic(Uuid, Rc<Fn(&mut C, Uuid)>),
    Once(Uuid, Box<dyn FnOnce(&mut C, Uuid)>),
}
*/

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(dead_code)]
    fn assert_event_timer_send_and_sync<E: Clone>(event_timer: &EventTimer<E>) {
        fn assert_send<T: Send>(_t: &T) {}
        fn assert_sync<T: Sync>(_t: &T) {}

        assert_send(event_timer);
        // TODO: Q: do we need event_timer to be Sync?
        //        assert_sync(event_timer);
    }
}
