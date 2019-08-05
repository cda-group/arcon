use core::time::Duration;
use kompact::timer::*;
use kompact::ComponentDefinition;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fmt;
use std::fmt::Debug;
use std::rc::Rc;
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

pub struct EventTimer<C: ComponentDefinition> {
    timer: QuadWheelWithOverflow,
    time: u64,
    handles: HashMap<Uuid, TimerHandle<C>>,
}

impl<C: ComponentDefinition> Timer<C> for EventTimer<C> {
    fn schedule_once<F>(&mut self, timeout: Duration, action: F) -> ()
    where
        F: FnOnce(&mut C, Uuid) + Send + 'static,
    {
        let id = Uuid::new_v4();
        let handle = TimerHandle::OneShot {
            _id: id,
            action: Box::new(action),
        };
        self.handles.insert(id, handle);

        let e = TimerEntry::OneShot {
            id: id,
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

    fn schedule_periodic<F>(&mut self, delay: Duration, period: Duration, action: F) -> ()
    where
        F: Fn(&mut C, Uuid) + Send + 'static,
    {
        let id = Uuid::new_v4();
        let handle = TimerHandle::Periodic {
            _id: id,
            action: Rc::new(action),
        };
        self.handles.insert(id, handle);

        let e = TimerEntry::Periodic {
            id: Uuid::new_v4(),
            delay,
            period,
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
    /* We don't implement cancel yet
    fn cancel(&mut self, id: Uuid) {
        match self.timer.cancel(id) {
            Ok(_) => (),                     // ok
            Err(TimerError::NotFound) => (), // also ok, might have been triggered already
            Err(f) => panic!("Unexpected error cancelling timer! {:?}", f),
        }
    } */
}

impl<C: ComponentDefinition> EventTimer<C> {
    pub fn new() -> EventTimer<C> {
        EventTimer {
            timer: QuadWheelWithOverflow::new(),
            time: 0u64,
            handles: HashMap::new(),
        }
    }
    // Our "unique" schedule function
    pub fn schedule_at<F>(&mut self, time: u64, action: F) -> ()
    where
        F: FnOnce(&mut C, Uuid) + Send + 'static,
    {
        // Check for bad target time
        if time < self.time {
            eprintln!("tried to schedule event which has already happened");
        } else {
            self.schedule_once(Duration::from_secs(time - self.time), action);
        }
    }
    /* use advance_to
    #[inline(always)]
    fn tick(&mut self) -> Vec<ExecuteAction<C>> {
        self.time = self.time + 1;
        let mut vec = Vec::new();
        for _ in 0..1000 {
            let mut res = self.timer.tick();
            for e in res.drain(..) {
                vec.push(self.execute(e));
            }
        }
        return vec;
    }
    */
    // Should be called before scheduling anything
    #[inline(always)]
    pub fn set_time(&mut self, ts: u64) -> () {
        self.time = ts;
    }
    pub fn get_time(&mut self) -> u64 {
        self.time
    }
    // Complex loop to skip in the wheel and always advance even number of seconds
    #[inline(always)]
    pub fn advance_to(&mut self, ts: u64) -> Vec<ExecuteAction<C>> {
        let mut vec = Vec::new();
        if ts <= self.time {
            eprintln!("advance_to called with lower timestamp than current time");
            return vec;
        }

        let mut time_left = ts - self.time;

        while time_left > 0 {
            if let Skip::Millis(skip_ms) = self.timer.can_skip() {
                // Only skip full-seconds
                let skip_seconds = skip_ms / 1000;
                if skip_seconds as u64 >= time_left {
                    // No more ops to gather, jump forward and return
                    self.timer.skip((time_left * 1000).try_into().unwrap());
                    self.time = self.time + time_left;
                    return vec;
                } else {
                    // Need to make sure we move forward full seconds
                    let skip_remainder = 1000 - (skip_ms % 1000);
                    self.timer.skip(skip_ms);
                    self.time = self.time + (skip_seconds as u64);
                    time_left = time_left - (skip_seconds as u64);
                    for _ in 0..skip_remainder {
                        let mut res = self.timer.tick();
                        for e in res.drain(..) {
                            vec.push(self.execute(e));
                        }
                    }
                    self.time = self.time + 1;
                    time_left = time_left - 1;
                }
            } else {
                // Can't skip, tick the full second
                for _ in 0..1000 {
                    let mut res = self.timer.tick();
                    for e in res.drain(..) {
                        vec.push(self.execute(e));
                    }
                }
                self.time = self.time + 1;
                time_left = time_left - 1;
            }
        }
        return vec;
    }
    // Takes TimerEntry, reschedules it if necessary and returns Executable actions
    #[inline(always)]
    fn execute(&mut self, e: TimerEntry) -> ExecuteAction<C> {
        let id = e.id();
        let res = self.handles.remove(&id);

        // Reschedule the event
        match e.execute() {
            Some(re_e) => match self.timer.insert(re_e) {
                Ok(_) => (), // great
                Err(TimerError::Expired(re_e)) => {
                    // This could happen if someone specifies 0ms period
                    eprintln!("TimerEntry could not be inserted properly: {:?}", re_e);
                }
                Err(f) => panic!("Could not insert timer entry! {:?}", f),
            },
            None => (), // great
        }

        match res {
            Some(TimerHandle::OneShot { action, .. }) => ExecuteAction::Once(id, action),
            Some(TimerHandle::Periodic { action, .. }) => {
                // Re-add the handle if it's periodic
                let action2 = action.clone();
                self.handles
                    .insert(id, TimerHandle::Periodic { _id: id, action });
                ExecuteAction::Periodic(id, action2)
            }
            _ => ExecuteAction::None,
        }
    }
}

impl<C: ComponentDefinition> Debug for EventTimer<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<EventTimer>")
    }
}

pub trait Timer<C: ComponentDefinition> {
    fn schedule_once<F>(&mut self, timeout: Duration, action: F) -> ()
    where
        F: FnOnce(&mut C, Uuid) + Send + 'static;

    fn schedule_periodic<F>(&mut self, delay: Duration, period: Duration, action: F) -> ()
    where
        F: Fn(&mut C, Uuid) + Send + 'static;
}

// Allows the EventTimer to be scheduled on different threads, but should never be used concurrently
unsafe impl<C: ComponentDefinition> Send for EventTimer<C> {}
unsafe impl<C: ComponentDefinition> Sync for EventTimer<C> {}

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

#[cfg(test)]
mod tests {}
