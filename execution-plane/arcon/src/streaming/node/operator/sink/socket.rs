// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::prelude::*;
use ::serde::Serialize;
use bytes::Bytes;
use futures::sync;
use futures::sync::mpsc;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::thread::{Builder, JoinHandle};
use tokio::net::UdpSocket;
use tokio::prelude::*;
use tokio::runtime::Runtime;
use tokio::runtime::TaskExecutor;

pub struct SocketSink<IN>
where
    IN: ArconType + Serialize,
{
    tx_channel: mpsc::Sender<Bytes>,
    executor: TaskExecutor,
    _handle: JoinHandle<()>,
    _marker: PhantomData<IN>,
}

impl<IN> SocketSink<IN>
where
    IN: ArconType + Serialize,
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
            tx_channel: tx,
            executor,
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

        let addr = "127.0.0.1:9999".parse().unwrap();
        let socket = UdpSocket::bind(&addr).unwrap();
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
