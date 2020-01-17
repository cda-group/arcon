use crate::prelude::*;
use ::serde::Serialize;
use bytes::Bytes;
use futures::{channel, StreamExt, SinkExt, TryFutureExt, executor::block_on};
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::thread::{Builder, JoinHandle};
use tokio::net::UdpSocket;
use tokio::prelude::*;
use tokio::runtime::{Runtime, Handle};

pub struct SocketSink<IN>
where
    IN: ArconType + Serialize,
{
    tx_channel: channel::mpsc::Sender<Bytes>,
    runtime_handle: Handle,
    _handle: JoinHandle<()>,
    _marker: PhantomData<IN>,
}

impl<IN> SocketSink<IN>
where
    IN: ArconType + Serialize,
{
    pub fn udp(socket_addr: SocketAddr) -> Self {
        let (tx, mut rx) = channel::mpsc::channel::<Bytes>(1_024);

        let (tx_exec, rx_exec) = channel::oneshot::channel();

        let th = Builder::new()
            .name(String::from("UdpSinkThread"))
            .spawn(move || {
                let mut runtime = Runtime::new().expect("Could not create Tokio Runtime!");
                let runtime_handle = runtime.handle().clone();

                runtime.block_on(async move {
                    // Let OS handle port alloc
                    let self_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
                    let mut socket = UdpSocket::bind(self_addr).await.expect("Failed to bind");

                    tx_exec.send(runtime_handle).expect("failed to send executor");

                    while let Some(bytes) = rx.next().await {
                        socket.send_to(&bytes, socket_addr).await;
                    }
                });
            })
            .map_err(|_| ())
            .unwrap();

        let runtime_handle = block_on(rx_exec).map_err(|_| ()).unwrap();

        SocketSink {
            tx_channel: tx,
            runtime_handle,
            _handle: th,
            _marker: PhantomData,
        }
    }
}

impl<IN> Operator<IN, IN> for SocketSink<IN>
where
    IN: ArconType + Serialize,
{
    fn handle_element(&mut self, e: ArconElement<IN>) -> ArconResult<Vec<ArconEvent<IN>>> {
        let mut tx = self.tx_channel.clone();
        let fmt_data = {
            if let Ok(mut json) = serde_json::to_string(&e.data) {
                json += "\n";
                json
            } else {
                format!("{:?}\n", e.data)
            }
        };
        let bytes = Bytes::from(fmt_data);
        let req_dispatch = async move {
            let res = tx.send(bytes).await;
            // TODO: ignored errors
        };
        self.runtime_handle.spawn(req_dispatch);
        Ok(Vec::new())
    }
    fn handle_watermark(&mut self, _w: Watermark) -> ArconResult<Vec<ArconEvent<IN>>> {
        Ok(Vec::new())
    }
    fn handle_epoch(&mut self, _epoch: Epoch) -> ArconResult<Vec<u8>> {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::util::mute_strategy;

    #[test]
    fn udp_sink_test() {
        let system = KompactConfig::default().build().expect("KompactSystem");
        const MAX_DATAGRAM_SIZE: usize = 65_507;
        let mut buf = vec![0u8; MAX_DATAGRAM_SIZE];

        let len = Runtime::new().expect("couln't create tokio runtime").block_on(async {
            let addr = "127.0.0.1:9999".parse().unwrap();
            let mut socket = UdpSocket::bind(&addr).await.unwrap();

            let socket_sink = system.create_and_start(move || {
                Node::new(
                    0.into(),
                    vec![1.into()],
                    mute_strategy::<i64>(),
                    Box::new(SocketSink::udp(addr)),
                )
            });

            std::thread::sleep(std::time::Duration::from_millis(100));

            let target: ActorRef<ArconMessage<i64>> = socket_sink.actor_ref();
            target.tell(ArconMessage::element(10 as i64, None, 1.into()));

            let (len, _) = socket.recv_from(&mut buf).await.expect("did not receive");
            len
        });

        let recv = String::from_utf8_lossy(&buf[..len]);
        assert_eq!(recv, String::from("10\n"));

        let _ = system.shutdown();
    }
}
