use crate::prelude::*;
use bytes::Bytes;
use futures::sync;
use futures::sync::mpsc;
use std::net::SocketAddr;
use std::thread::{Builder, JoinHandle};
use tokio::net::UdpSocket;
use tokio::prelude::*;
use tokio::runtime::Runtime;
use tokio::runtime::TaskExecutor;

#[derive(ComponentDefinition)]
pub struct SocketSink<A>
where
    A: ArconType + 'static,
{
    ctx: ComponentContext<Self>,
    tx_channel: mpsc::Sender<Bytes>,
    executor: TaskExecutor,
    _handle: JoinHandle<()>,
}

impl<A> SocketSink<A>
where
    A: ArconType + 'static,
{
    pub fn udp(socket_addr: SocketAddr) -> Self {
        let (tx, rx) = mpsc::channel::<Bytes>(1_024);

        let (tx_exec, rx_exec) = sync::oneshot::channel();

        let th = Builder::new()
            .name(String::from("UdpSinkThread"))
            .spawn(move || {
                let mut runtime = Runtime::new().expect("Could not create Tokio Runtime!");
                let executor = runtime.executor();
                // Let OS handle port alloc
                let self_addr = "0.0.0.0:0".parse().unwrap();
                let socket = UdpSocket::bind(&self_addr).expect("Failed to bind");
                socket.connect(&socket_addr).expect("Failed connection");

                let handler = rx
                    .fold(socket, move |s, b| {
                        s.send_dgram(b, &socket_addr)
                            .map(move |(ret_socket, _)| ret_socket)
                            .map_err(|_| ())
                    })
                    .map(|_| ());

                runtime.spawn(handler);
                tx_exec.send(executor).expect("failed to send executor");
                runtime.shutdown_on_idle().wait().unwrap();
            })
            .map_err(|_| ())
            .unwrap();

        let executor = rx_exec.wait().map_err(|_| ()).unwrap();

        SocketSink {
            ctx: ComponentContext::new(),
            tx_channel: tx.clone(),
            executor,
            _handle: th,
        }
    }

    fn handle_event(&mut self, event: &ArconEvent<A>) {
        match event {
            ArconEvent::Element(e) => {
                debug!(self.ctx.log(), "Sink element: {:?}", e.data);
                let tx = self.tx_channel.clone();
                let fmt_data = format!("{:?}\n", e.data);
                let bytes = Bytes::from(fmt_data);
                let req_dispatch = tx.send(bytes).map_err(|_| ()).and_then(|_| Ok(()));
                self.executor.spawn(req_dispatch);
            }
            _ => {}
        }
    }
}

impl<A> Provide<ControlPort> for SocketSink<A>
where
    A: ArconType + 'static,
{
    fn handle(&mut self, _event: ControlEvent) -> () {}
}

impl<A> Actor for SocketSink<A>
where
    A: ArconType + 'static,
{
    fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
        if let Some(event) = msg.downcast_ref::<ArconEvent<A>>() {
            self.handle_event(event);
        }
    }
    fn receive_message(&mut self, _sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {
        unimplemented!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn udp_sink_test() {
        let cfg = KompactConfig::new();
        let system = KompactSystem::new(cfg).expect("KompactSystem");

        let addr = "127.0.0.1:9999".parse().unwrap();
        let socket = UdpSocket::bind(&addr).unwrap();
        let socket_sink = system.create_and_start(move || {
            let s: SocketSink<i64> = SocketSink::udp(addr);
            s
        });

        std::thread::sleep(std::time::Duration::from_millis(100));

        let target = socket_sink.actor_ref();
        let e1 = ArconElement::new(10 as i64);
        target.tell(Box::new(ArconEvent::Element(e1)), &target);

        const MAX_DATAGRAM_SIZE: usize = 65_507;

        let _ = socket
            .recv_dgram(vec![0u8; MAX_DATAGRAM_SIZE])
            .map(|(_, data, len, _)| {
                let recv = String::from_utf8_lossy(&data[..len]);
                assert_eq!(recv, String::from("10\n"))
            })
            .wait();

        let _ = system.shutdown();
    }
}
