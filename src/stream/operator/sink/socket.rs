// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    data::{ArconElement, ArconNever, ArconType},
    error::ArconResult,
    stream::operator::{Operator, OperatorContext},
};
use ::serde::Serialize;
use arcon_state::Backend;
use bytes::Bytes;
use futures::{channel, executor::block_on, SinkExt, StreamExt};
use kompact::prelude::*;
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
    op_state: (),
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
                let runtime = Runtime::new().expect("Could not create Tokio Runtime!");
                let runtime_handle = runtime.handle().clone();

                runtime.block_on(async move {
                    // Let OS handle port alloc
                    let self_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
                    let socket = UdpSocket::bind(self_addr).await.expect("Failed to bind");

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
            op_state: (),
            _handle: th,
            _marker: PhantomData,
        }
    }
}

impl<IN> Operator for SocketSink<IN>
where
    IN: ArconType + Serialize,
{
    type IN = IN;
    type OUT = ArconNever;
    type TimerState = ArconNever;
    type OperatorState = ();

    fn handle_element(
        &mut self,
        element: ArconElement<Self::IN>,
        _ctx: OperatorContext<Self, impl Backend, impl ComponentDefinition>,
    ) -> ArconResult<()> {
        let mut tx = self.tx_channel.clone();
        let fmt_data = {
            if let Ok(mut json) = serde_json::to_string(&element.data) {
                json += "\n";
                json
            } else {
                format!("{:?}\n", element.data)
            }
        };
        let bytes = Bytes::from(fmt_data);
        let req_dispatch = async move {
            let res = tx.send(bytes).await;
            if let Err(e) = res {
                panic!("{:?}", e);
            }
        };
        self.runtime_handle.spawn(req_dispatch);
        Ok(())
    }
    crate::ignore_timeout!();
    crate::ignore_persist!();
    fn state(&mut self) -> &mut Self::OperatorState {
        &mut self.op_state
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;
    use std::{net::SocketAddr, sync::Arc};

    #[test]
    fn udp_sink_test() {
        const MAX_DATAGRAM_SIZE: usize = 65_507;
        let mut buf = vec![0u8; MAX_DATAGRAM_SIZE];
        let len = Runtime::new()
            .expect("couln't create tokio runtime")
            .block_on(async {
                let addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
                let mut pipeline = Pipeline::default()
                    .collection(vec![10], |conf| {
                        conf.set_arcon_time(ArconTime::Process);
                    })
                    .operator(OperatorBuilder {
                        constructor: Arc::new(move |_| SocketSink::udp(addr)),
                        conf: OperatorConf {
                            parallelism_strategy: ParallelismStrategy::Static(1),
                            ..Default::default()
                        },
                    })
                    .build();

                let socket = UdpSocket::bind(&addr).await.unwrap();
                pipeline.start();
                let (len, _) = socket.recv_from(&mut buf).await.expect("did not receive");
                len
            });

        let recv = String::from_utf8_lossy(&buf[..len]);
        assert_eq!(recv, String::from("10\n"));
    }
}
