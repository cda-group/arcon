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
                let runtime = Runtime::new().expect("Could not create Tokio Runtime!");
                let executor = runtime.executor();
                // Let OS handle port alloc
                let self_addr = "0.0.0.0:0".parse().unwrap();
                let socket = UdpSocket::bind(&self_addr).expect("Failed to bind");

                let handler = rx
                    .fold(socket, move |s, b| {
                        s.send_dgram(b, &socket_addr)
                            .map(move |(ret_socket, _)| ret_socket)
                            .map_err(|_| ())
                    })
                    .map(|_| ());

                tx_exec.send(executor).expect("failed to send executor");
                tokio::run(handler);
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
                let fmt_data = {
                    if let Ok(mut json) = serde_json::to_string(&e.data) {
                        json += "\n";
                        json
                    } else {
                        format!("{:?}\n", e.data)
                    }
                };
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
    type Message = ArconMessage<A>;
    fn receive_local(&mut self, msg: Self::Message) {
        self.handle_event(&msg.event);
    }
    fn receive_network(&mut self, _msg: NetMessage) {
        unimplemented!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn udp_sink_test() {
        let system = KompactConfig::default().build().expect("KompactSystem");

        let addr = "127.0.0.1:9999".parse().unwrap();
        let socket = UdpSocket::bind(&addr).unwrap();
        let socket_sink = system.create_and_start(move || {
            let s: SocketSink<i64> = SocketSink::udp(addr);
            s
        });

        std::thread::sleep(std::time::Duration::from_millis(100));

        let target: ActorRef<ArconMessage<i64>> = socket_sink.actor_ref();
        target.tell(ArconMessage::element(10 as i64, None, 0.into()));

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
