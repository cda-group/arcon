use core::time::Duration;
use kompact::ScheduledTimer;
use std::collections::HashMap;
use std::rc::Rc;
//use kompact::timer::Timer;
use kompact::timer::*;

use kompact::ComponentDefinition;
use std::fmt;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::time::Instant;
use uuid::Uuid;

#[derive(Debug)]
enum TimerMsg {
    Schedule(TimerEntry),
    Cancel(Uuid),
    Stop,
}

pub struct EventTimer<C: ComponentDefinition> {
    timer: QuadWheelWithOverflow,
    time: u64,
    handles: HashMap<Uuid, TimerHandle<C>>,
    _c: PhantomData<C>,
}

impl<C: ComponentDefinition> Timer<C> for EventTimer<C> {
    fn schedule_once<F>(&mut self, timeout: Duration, action: F) -> ()
    where
        F: FnOnce(&mut C, Uuid) + Send + 'static,
    {
        println!("event_timer trying schedule_once!");
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
        println!("event_timer trying to schedule_periodic!");
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
    /*
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
            _c: PhantomData,
        }
    }
    pub fn schedule_at<F>(&mut self, time: u64, action: F) -> ()
    where
        F: FnOnce(&mut C, Uuid) + Send + 'static,
    {
        self.schedule_once(Duration::from_secs(time - self.time), action)
    }
    #[inline(always)]
    fn tick(&mut self) -> () {
        self.time = self.time + 1;
        for _ in 0..1000 {
            let mut res = self.timer.tick();
            for e in res.drain(..) {
                self.execute(e);
            }
        }
    }
    #[inline(always)]
    pub fn set_time(&mut self, ts: u64) -> () {
        self.time = ts;
        println!("\nevent_timer set time to {}\n", self.time);
    }
    #[inline(always)]
    pub fn tick_to(&mut self, ts: u64) -> () {
        for e in self.time..ts {
            self.tick();
        }
    }
    #[inline(always)]
    fn execute(&mut self, e: TimerEntry) -> () {
        println!("\nevent_timer trying to execute!\n");
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

// Please don't share the EventTimer between threads, its not safe
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
