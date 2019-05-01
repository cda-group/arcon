extern crate tokio_threadpool;

use futures::future::Future;
use kompact::*;
use std::fmt;
use std::sync::Arc;
use tokio_threadpool::ThreadPool;

pub trait Launcher: Sync + Send {}
impl<E: Launcher + ?Sized> Launcher for Box<E> {}
//type Fuut = Box<Future<Item = (), Error = ()> + Send>;
type Fut = Arc<Box<Future<Item = (), Error = ()> + Send + Sync>>;
//type Fut = Box<dyn Future<Item = (), Error = ()> + 'static + Send + Sync>;

#[derive(Clone)]
struct IOFuture(Fut);
//struct IOFuture(Arc<Future<Item = (), Error = ()> + 'static + Send + Sync>);
//struct IOFuture(Arc<Box<Future<Item = (), Error = ()> + 'static + Send + Sync>>);

impl fmt::Debug for IOFuture {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "iofuture")
    }
}

struct IOPort;

impl Port for IOPort {
    type Indication = ();
    type Request = IOFuture;
}

pub struct WeldPort;

impl Port for WeldPort {
    type Indication = ();
    type Request = ();
}

#[derive(ComponentDefinition, Actor)]
pub struct Scheduler {
    ctx: ComponentContext<Scheduler>,
    io_port: ProvidedPort<IOPort, Scheduler>,
    weld_port: ProvidedPort<WeldPort, Scheduler>,
    _executor: ThreadPool,
}

impl Scheduler {
    pub fn new() -> Scheduler {
        Scheduler {
            ctx: ComponentContext::new(),
            io_port: ProvidedPort::new(),
            weld_port: ProvidedPort::new(),
            _executor: ThreadPool::new(),
        }
    }
}

impl Provide<ControlPort> for Scheduler {
    fn handle(&mut self, event: ControlEvent) -> () {
        if let ControlEvent::Start = event {
            info!(self.ctx.log(), "Starting Scheduler");
        }
    }
}

impl Provide<IOPort> for Scheduler {
    fn handle(&mut self, _event: IOFuture) {
        //let s = Arc::try_unwrap(_event.0);
        //self._executor.spawn(_event.0
    }
}

impl Provide<WeldPort> for Scheduler {
    fn handle(&mut self, _event: ()) {}
}
