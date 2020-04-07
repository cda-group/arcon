// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{prelude::*, stream::operator::OperatorContext};
use ::serde::Serialize;
use bytes::Bytes;
use futures::{channel, executor::block_on, SinkExt, StreamExt};
use std::{
    marker::PhantomData,
    net::SocketAddr,
    thread::{Builder, JoinHandle},
};
use tokio::{
    net::UdpSocket,
    runtime::{Handle, Runtime},
};

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

                    tx_exec
                        .send(runtime_handle)
                        .expect("failed to send executor");

                    while let Some(bytes) = rx.next().await {
                        socket
                            .send_to(&bytes, socket_addr)
                            .await
                            .expect("send failed");
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
    fn handle_element(&mut self, e: ArconElement<IN>, _ctx: OperatorContext<IN>) {
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

            match res {
                Ok(_) => {}  // everything ok
                Err(_) => {} // ignore errors
            }
        };
        self.runtime_handle.spawn(req_dispatch);
    }
    fn handle_watermark(
        &mut self,
        _w: Watermark,
        _ctx: OperatorContext<IN>,
    ) -> Option<Vec<ArconEvent<IN>>> {
        None
    }
    fn handle_epoch(
        &mut self,
        _epoch: Epoch,
        _ctx: OperatorContext<IN>,
    ) -> Option<ArconResult<Vec<u8>>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::ChannelStrategy;

    #[test]
    fn udp_sink_test() {
        let system = KompactConfig::default().build().expect("KompactSystem");
        const MAX_DATAGRAM_SIZE: usize = 65_507;
        let mut buf = vec![0u8; MAX_DATAGRAM_SIZE];

        let len = Runtime::new()
            .expect("couln't create tokio runtime")
            .block_on(async {
                let addr = "127.0.0.1:9999".parse().unwrap();
                let mut socket = UdpSocket::bind(&addr).await.unwrap();

                let socket_sink = system.create(move || {
                    Node::new(
                        0.into(),
                        vec![1.into()],
                        ChannelStrategy::Mute,
                        Box::new(SocketSink::udp(addr)),
                        Box::new(InMemory::new("test").unwrap()),
                        ".".into(),
                    )
                });
                system.start(&socket_sink);
                system.start(&socket_sink);

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
