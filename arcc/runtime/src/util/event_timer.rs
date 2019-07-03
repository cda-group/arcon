use core::time::Duration;
use kompact::timer::Timer;
use kompact::timer::*;
use std::fmt;
use std::fmt::Debug;
use std::time::Instant;
use uuid::Uuid;

#[derive(Debug)]
enum TimerMsg {
    Schedule(TimerEntry),
    Cancel(Uuid),
    Stop,
}

// Please don't share the EventTimer between threads, its not safe
unsafe impl Send for EventTimer {}
unsafe impl Sync for EventTimer {}

impl Timer for EventTimer {
    fn schedule_once<F>(&mut self, id: Uuid, timeout: Duration, action: F) -> ()
    where
        F: FnOnce(Uuid) + Send + 'static,
    {
        let e = TimerEntry::OneShot {
            id,
            timeout,
            action: Box::new(action),
        };
        match self.timer.insert(e) {
            Ok(_) => (), // ok
            Err(TimerError::Expired(e)) => {
                self.execute(e);
            }
            Err(f) => panic!("Could not insert timer entry! {:?}", f),
        }
    }

    fn schedule_periodic<F>(&mut self, id: Uuid, delay: Duration, period: Duration, action: F) -> ()
    where
        F: Fn(Uuid) + Send + 'static,
    {
        let e = TimerEntry::Periodic {
            id,
            delay,
            period,
            action: Box::new(action),
        };
        match self.timer.insert(e) {
            Ok(_) => (), // ok
            Err(TimerError::Expired(e)) => {
                self.execute(e);
            }
            Err(f) => panic!("Could not insert timer entry! {:?}", f),
        }
    }
    fn cancel(&mut self, id: Uuid) {
        match self.timer.cancel(id) {
            Ok(_) => (),                     // ok
            Err(TimerError::NotFound) => (), // also ok, might have been triggered already
            Err(f) => panic!("Unexpected error cancelling timer! {:?}", f),
        }
    }
}

pub struct EventTimer {
    timer: QuadWheelWithOverflow,
    time: u64,
}

impl EventTimer {
    pub fn new() -> EventTimer {
        EventTimer {
            timer: QuadWheelWithOverflow::new(),
            time: 0u64,
        }
    }
    pub fn schedule_at<F>(&mut self, id: Uuid, time: u64, action: F) -> ()
    where
        F: FnOnce(Uuid) + Send + 'static,
    {
        self.schedule_once(id, Duration::from_secs(time - self.time), action)
    }
    #[inline(always)]
    fn tick(&mut self) -> () {
        self.time = self.time + 1;
        let mut res = self.timer.tick();
        for e in res.drain(..) {
            self.execute(e);
        }
    }
    #[inline(always)]
    pub fn set_time(&mut self, ts: u64) -> () {
        self.time = ts;
    }
    #[inline(always)]
    pub fn tick_to(&mut self, ts: u64) -> () {
        for e in self.time..ts {
            self.tick();
        }
    }
    #[inline(always)]
    fn execute(&mut self, e: TimerEntry) -> () {
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
    }
}

impl Debug for EventTimer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<EventTimer>")
    }
}
