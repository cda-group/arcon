extern crate tokio_threadpool;

use futures::future::Future;
use kompact::*;
use std::fmt;
use std::sync::Arc;
use tokio_threadpool::ThreadPool;

#[derive(Clone, Debug)]
struct Ping;
#[derive(Clone, Debug)]
struct Pong;

#[derive(Clone)]
struct IOFuture(Arc<Future<Item = (), Error = ()> + 'static + Send + Sync>);

impl fmt::Debug for IOFuture {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "iofuture")
    }
}

struct IOPort;

impl Port for IOPort {
    type Indication = Pong;
    type Request = IOFuture;
}

#[derive(ComponentDefinition, Actor)]
pub struct Dispatcher {
    ctx: ComponentContext<Dispatcher>,
    io_port: ProvidedPort<IOPort, Dispatcher>,
    _executor: ThreadPool,
}

impl Dispatcher {
    pub fn new() -> Dispatcher {
        Dispatcher {
            ctx: ComponentContext::new(),
            io_port: ProvidedPort::new(),
            _executor: ThreadPool::new(),
        }
    }
}

impl Provide<ControlPort> for Dispatcher {
    fn handle(&mut self, event: ControlEvent) -> () {
        if let ControlEvent::Start = event {
            info!(self.ctx.log(), "Starting Dispatcher");
        }
    }
}

impl Provide<IOPort> for Dispatcher {
    fn handle(&mut self, _event: IOFuture) {
        //self.executor.spawn(event.0);
    }
}
