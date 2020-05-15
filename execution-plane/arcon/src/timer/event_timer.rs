// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use super::*;
use crate::prelude::{
    state::{Handle, MapState, ValueState},
    *,
};
use arcon_state::{Bundle, Session};
use core::time::Duration;
use hierarchical_hash_wheel_timer::{
    wheels::{quad_wheel::*, *},
    *,
};
use prost::Message;
#[cfg(feature = "arcon_serde")]
use serde::{Deserialize, Serialize};
use std::{fmt, fmt::Debug};
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

    Usage is thus to store the timer within a component, schedule events on it and using
        advance_to to return ordered set of actions to perform.
*/

pub trait TimerTypeBoundsNoSerde: Message + Default + PartialEq + Clone + 'static {}

impl<T> TimerTypeBoundsNoSerde for T where T: Message + Default + PartialEq + Clone + 'static {}

cfg_if::cfg_if! {
    if #[cfg(feature = "arcon_serde")] {
        pub trait TimerTypeBounds: TimerTypeBoundsNoSerde + Serialize + for<'de> Deserialize<'de> {}
        impl<T> TimerTypeBounds for T where T: TimerTypeBoundsNoSerde + Serialize + for<'de> Deserialize<'de> {}
    } else {
        pub trait TimerTypeBounds: TimerTypeBoundsNoSerde {}
        impl<T> TimerTypeBounds for T where T: TimerTypeBoundsNoSerde {}
    }
}

#[cfg_attr(feature = "arcon_serde", derive(Serialize, Deserialize))]
#[derive(Message, PartialEq, Clone)]
#[cfg_attr(feature = "arcon_serde", serde(bound = "E: TimerTypeBounds"))]
pub struct EventTimerEvent<E: TimerTypeBounds> {
    #[prost(uint64, tag = "1")]
    time_when_scheduled: u64,
    #[prost(uint64, tag = "2")]
    timeout_millis: u64,
    #[prost(message, required, tag = "3")]
    payload: E,
}

impl<E: TimerTypeBounds> EventTimerEvent<E> {
    fn new(time_when_scheduled: u64, timeout_millis: u64, payload: E) -> Self {
        EventTimerEvent {
            time_when_scheduled,
            timeout_millis,
            payload,
        }
    }
}

arcon_state::bundle! {
    /// Internal Node State
    struct TimerState<E: TimerTypeBounds> {
        current_time: Handle<ValueState<u64>>,
        timeouts: Handle<MapState<String, EventTimerEvent<E>>>,
    }
}

impl<E: TimerTypeBounds> TimerState<E> {
    fn new() -> TimerState<E> {
        TimerState {
            current_time: Handle::value("__event_timer_current_time"),
            timeouts: Handle::map("__event_timer_timeouts"),
        }
    }
}

pub struct EventTimer<E: TimerTypeBounds> {
    // Since the backing store uses string keys, there's no point in storing Uuids here and converting back and forth
    timer: QuadWheelWithOverflow<String>,
    state: TimerState<E>,
}
impl<E: TimerTypeBounds> EventTimer<E> {
    pub fn new() -> EventTimer<E> {
        let timer = EventTimer {
            timer: QuadWheelWithOverflow::default(),
            state: TimerState::new(),
        };
        timer
    }

    pub fn with_state<'this, 'session, 'backend, B: state::Backend>(
        &'this mut self,
        state_session: &'session mut Session<'backend, B>,
    ) -> EventTimerWithState<'this, 'session, 'backend, E, B>
    where
        'backend: 'session,
    {
        EventTimerWithState {
            timer: &mut self.timer,
            state: self.state.activate(state_session),
        }
    }
}

pub struct EventTimerWithState<'this, 'session, 'backend, E, B>
where
    E: TimerTypeBounds,
    B: state::Backend,
    'backend: 'session,
{
    timer: &'this mut QuadWheelWithOverflow<String>,
    state: <TimerState<E> as Bundle<'this, 'session, 'backend, B>>::Active,
}

impl<E: TimerTypeBounds, B: state::Backend> EventTimerWithState<'_, '_, '_, E, B> {
    fn replay_events(&mut self) {
        let time = self.current_time();

        for res in self
            .state
            .timeouts()
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
    }

    fn set_time(&mut self, ts: u64) {
        self.state
            .current_time()
            .fast_set(ts)
            .expect("could not set current time");
    }

    fn add_time(&mut self, by: u64) {
        let cur = self.current_time();
        let new_time = cur + by;
        self.set_time(new_time);
    }

    fn tick_and_collect(&mut self, mut time_left: u32, res: &mut Vec<E>) -> () {
        // TODO 1) optimise access pattern to not hit storage on every iteration
        // TODO 2) The we are handling timeouts and timestamps separately can produce a lot of torn writes...
        // we may need to change this if it produces data consistency issues
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
    fn take_entry(&mut self, id: String) -> Option<E> {
        //self.inner.handles.remove(&id).map(|x| x.payload)
        self.state
            .timeouts()
            .remove(&id)
            .expect("no timeout found for id") // this wouldn't necessarily be an error anymore if we add a cancellation API at some point
            .map(|e| e.payload)
    }

    #[inline(always)]
    fn put_entry(&mut self, id: String, e: EventTimerEvent<E>) {
        self.state
            .timeouts()
            .fast_insert(id, e)
            .expect("couldn't persist timeout");
    }

    fn schedule_after(&mut self, delay: u64, entry: E) -> Result<(), E> {
        // this seems a bit silly, but it is A way to generate a unique string, I suppose^^
        let id = Uuid::new_v4().to_string();

        match self
            .timer
            .insert_with_delay(id.clone(), Duration::from_millis(delay))
        {
            Ok(_) => {
                let e = EventTimerEvent::new(self.current_time(), delay, entry);
                self.put_entry(id, e);
                Ok(())
            }
            Err(TimerError::Expired(_)) => Err(entry),
            Err(f) => panic!("Could not insert timer entry! {:?}", f),
        }
    }
    fn schedule_at(&mut self, time: u64, entry: E) -> Result<(), E> {
        let cur_time = self.current_time();
        // Check for expired target time
        if time <= cur_time {
            Err(entry)
        } else {
            self.schedule_after(time - cur_time, entry)
        }
    }

    fn current_time(&mut self) -> u64 {
        self.state
            .current_time()
            .get()
            .expect("couldn't get current time")
            .unwrap()
    }

    fn advance_to(&mut self, ts: u64) -> Vec<E> {
        let mut res = Vec::new();
        let cur_time = self.current_time();
        if ts < cur_time {
            eprintln!("advance_to called with lower timestamp than current time");
            return res;
        }

        let mut time_left = ts - cur_time;
        while time_left > std::u32::MAX as u64 {
            self.tick_and_collect(std::u32::MAX, &mut res);
            time_left -= std::u32::MAX as u64;
        }
        // this cast must be safe now
        self.tick_and_collect(time_left as u32, &mut res);
        res
    }
}

impl<E: TimerTypeBounds> TimerBackend<E> for EventTimer<E> {
    fn register_states(
        &mut self,
        registration_token: &mut state::RegistrationToken<impl state::Backend>,
    ) {
        self.state.register_states(registration_token)
    }

    fn init(&mut self, session: &mut state::Session<impl state::Backend>) {
        let mut state = self.state.activate(session);
        let mut current_time = state.current_time();

        if current_time
            .get()
            .expect("could not check current time")
            .is_none()
        {
            current_time.set(0u64).expect("could not set current time");
        }

        let mut twt = self.with_state(session);
        twt.replay_events(); // in case something was already in the storage
    }

    fn schedule_after(
        &mut self,
        delay: u64,
        entry: E,
        state_session: &mut state::Session<impl state::Backend>,
    ) -> Result<(), E> {
        self.with_state(state_session).schedule_after(delay, entry)
    }

    fn schedule_at(
        &mut self,
        time: u64,
        entry: E,
        state_session: &mut state::Session<impl state::Backend>,
    ) -> Result<(), E> {
        self.with_state(state_session).schedule_at(time, entry)
    }

    fn current_time(&mut self, state_session: &mut state::Session<impl state::Backend>) -> u64 {
        self.with_state(state_session).current_time()
    }

    fn advance_to(
        &mut self,
        ts: u64,
        state_session: &mut state::Session<impl state::Backend>,
    ) -> Vec<E> {
        self.with_state(state_session).advance_to(ts)
    }

    fn handle_epoch(
        &mut self,
        _epoch: Epoch,
        _state_session: &mut state::Session<impl state::Backend>,
    ) {
        () // do absolutely nothing
    }
}

impl<E: TimerTypeBounds> Debug for EventTimer<E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<EventTimer>")
    }
}
