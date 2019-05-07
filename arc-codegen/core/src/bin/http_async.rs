extern crate core;
#[cfg(feature = "http")]
extern crate http;

use core::components::io::*;
use core::prelude::future::Future;
use core::prelude::*;
use http::client::HttpsClient;
use tokio_threadpool::ThreadPool;

fn main() {
    let system = KompactConfig::default().build().expect("KompactSystem");

    let operator = system.create(Operator::new);
    system.start(&operator);

    let tcp_source = system.create_and_register(move || TcpSource::new(operator.actor_ref()));
    system.start(&tcp_source.0);
    system.await_termination();
}

#[derive(ComponentDefinition)]
pub struct Operator {
    ctx: ComponentContext<Operator>,
    https_client: HttpsClient,
    pool: ThreadPool,
}

impl Operator {
    pub fn new() -> Operator {
        Operator {
            ctx: ComponentContext::new(),
            https_client: HttpsClient::new(),
            pool: ThreadPool::new(),
        }
    }
}

impl Actor for Operator {
    fn receive_local(&mut self, _sender: ActorRef, msg: Box<Any>) {
        if let Ok(key) = msg.downcast::<String>() {
            info!(self.ctx.log(), "Operator received key: {}", key);
            let url = {
                format!("https://en.wikipedia.org/w/api.php?action=opensearch&search={}&limit=1&format=json", key.replace('\n', ""))
            };
            let req = self.https_client.get(url);

            self.pool.spawn({
                req.and_then(|res| {
                    println!("{}", res.status());
                    Ok(())
                })
                .map_err(|e| panic!("err={:?}", e))
            });
        }
    }
    fn receive_message(&mut self, sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {
        error!(self.ctx.log(), "Got unexpected message from {}", sender);
    }
}

impl Provide<ControlPort> for Operator {
    fn handle(&mut self, event: ControlEvent) {
        if let ControlEvent::Start = event {}
    }
}

#[derive(ComponentDefinition)]
pub struct TcpSource {
    ctx: ComponentContext<TcpSource>,
    operator: ActorRef,
}

impl TcpSource {
    pub fn new(target: ActorRef) -> TcpSource {
        TcpSource {
            ctx: ComponentContext::new(),
            operator: target,
        }
    }
}

impl Actor for TcpSource {
    fn receive_local(&mut self, _sender: ActorRef, msg: Box<Any>) {
        if let Some(ref recv) = msg.downcast_ref::<TcpRecv>() {
            let key = String::from_utf8_lossy(&recv.bytes).into_owned();

            if key.len() > 1 {
                self.operator.tell(Box::new(key), self);
            }
        } else if let Some(ref _close) = msg.downcast_ref::<TcpClosed>() {
            info!(self.ctx.log(), "TCP connection closed");
        } else if let Some(ref _err) = msg.downcast_ref::<TcpErr>() {
            error!(self.ctx.log(), "TCP IO Error");
        }
    }
    fn receive_message(&mut self, sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {
        error!(self.ctx.log(), "Got unexpected message from {}", sender);
    }
}

impl Provide<ControlPort> for TcpSource {
    fn handle(&mut self, event: ControlEvent) {
        if let ControlEvent::Start = event {
            let port = 3000;
            let system = self.ctx.system();
            system.create_and_start(move || IO::new(port, self.actor_ref(), IOKind::Tcp));
        }
    }
}
