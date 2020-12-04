// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    state,
    stream::{operator::Operator, source::SourceContext},
    util::io::*,
};
use kompact::prelude::*;
use std::{
    cell::RefCell,
    net::SocketAddr,
    str::{from_utf8, FromStr},
    time::Duration,
};

pub enum SocketKind {
    Tcp,
    Udp,
}

#[derive(ComponentDefinition)]
pub struct SocketSource<OP, B>
where
    OP: Operator + 'static,
    OP::IN: FromStr,
    B: state::Backend,
{
    ctx: ComponentContext<Self>,
    source_ctx: RefCell<SourceContext<OP, B>>,
    sock_addr: SocketAddr,
    sock_kind: SocketKind,
}

impl<OP, B> SocketSource<OP, B>
where
    OP: Operator + 'static,
    OP::IN: FromStr,
    B: state::Backend,
{
    pub fn new(
        sock_addr: SocketAddr,
        sock_kind: SocketKind,
        source_ctx: SourceContext<OP, B>,
    ) -> Self {
        assert!(source_ctx.watermark_interval > 0);
        SocketSource {
            ctx: ComponentContext::uninitialised(),
            source_ctx: RefCell::new(source_ctx),
            sock_addr,
            sock_kind,
        }
    }
}

impl<OP, B> ComponentLifecycle for SocketSource<OP, B>
where
    OP: Operator + 'static,
    OP::IN: FromStr,
    B: state::Backend,
{
    fn on_start(&mut self) -> Handled {
        let system = self.ctx.system();
        let watermark_interval = self.source_ctx.borrow_mut().watermark_interval;
        // Schedule periodic watermark generation
        self.schedule_periodic(
            Duration::from_secs(0),
            Duration::from_secs(watermark_interval),
            move |self_c, _| {
                self_c.source_ctx.borrow_mut().generate_watermark(self_c);
                Handled::Ok
            },
        );

        match self.sock_kind {
            SocketKind::Tcp => {
                let comp = system.create(move || IO::tcp(self.sock_addr, self.actor_ref()));
                system.start(&comp);
            }
            SocketKind::Udp => {
                let comp = system.create(move || IO::udp(self.sock_addr, self.actor_ref()));
                system.start(&comp);
            }
        }
        Handled::Ok
    }
}

impl<OP, B> Actor for SocketSource<OP, B>
where
    OP: Operator + 'static,
    OP::IN: FromStr,
    B: state::Backend,
{
    type Message = IOMessage;

    fn receive_local(&mut self, msg: Self::Message) -> Handled {
        match msg {
            IOMessage::Bytes(bytes) => {
                let mut source_ctx = self.source_ctx.borrow_mut();
                debug!(self.ctx.log(), "{:?}", bytes);
                if let Ok(byte_string) = from_utf8(&bytes) {
                    if let Ok(in_data) = byte_string.trim().parse::<OP::IN>() {
                        let elem = source_ctx.extract_element(in_data);
                        if let Err(err) = source_ctx.process(elem, self) {
                            error!(self.ctx.log(), "Error while processing record {:?}", err);
                        }
                    } else {
                        error!(self.ctx.log(), "Unable to parse string {}", byte_string);
                    }
                }
            }
            IOMessage::SockClosed => info!(self.ctx.log(), "Sock connection closed"),
            IOMessage::SockErr => error!(self.ctx.log(), "Sock IO Error"),
        }
        Handled::Ok
    }

    fn receive_network(&mut self, _msg: NetMessage) -> Handled {
        error!(self.ctx.log(), "Got unexpected message");
        Handled::Ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        pipeline::Pipeline,
        prelude::{Channel, ChannelStrategy, DebugNode, Forward, Map},
    };
    use std::{sync::Arc, thread, time};
    use tokio::{net::TcpStream, prelude::*, runtime::Runtime};

    // Shared methods for test cases
    fn wait(time: u64) {
        thread::sleep(time::Duration::from_secs(time));
    }

    #[test]
    fn timestamp_extraction_test() {
        // Our example data struct for this test case.
        // add arcon_decoder to decode from String
        #[arcon_decoder(,)]
        #[cfg_attr(feature = "arcon_serde", derive(serde::Serialize, serde::Deserialize))]
        #[derive(Arcon, prost::Message, Clone, abomonation_derive::Abomonation)]
        #[arcon(unsafe_ser_id = 500, reliable_ser_id = 501, version = 1)]
        pub struct ExtractorStruct {
            #[prost(uint32, tag = "1")]
            data: u32,
            #[prost(uint64, tag = "2")]
            timestamp: u64,
        }

        // Setup conf
        let addr = "127.0.0.1:4001".parse().unwrap();

        // Setup
        let mut pipeline = Pipeline::new();
        let pool_info = pipeline.get_pool_info();
        let system = pipeline.system();

        let sink_comp = system.create(DebugNode::<ExtractorStruct>::new);

        system
            .start_notify(&sink_comp)
            .wait_timeout(std::time::Duration::from_millis(100))
            .expect("started");

        let sink_ref = sink_comp
            .actor_ref()
            .hold()
            .expect("Failed to fetch strong ref");
        let channel_strategy =
            ChannelStrategy::Forward(Forward::new(Channel::Local(sink_ref), 1.into(), pool_info));

        // just pass it on
        fn map_fn(x: ExtractorStruct) -> ExtractorStruct {
            x
        }

        // Set up SourceContext
        let watermark_interval = 1; // in seconds currently for SocketSource

        fn timestamp_extractor(x: &ExtractorStruct) -> u64 {
            x.timestamp
        }
        let backend = Arc::new(crate::util::temp_backend());

        let source_context = SourceContext::new(
            watermark_interval,
            Some(&timestamp_extractor),
            channel_strategy,
            Map::new(&map_fn),
            backend,
        );

        let socket_source = SocketSource::new(addr, SocketKind::Tcp, source_context);
        let source_comp = system.create(move || socket_source);

        system
            .start_notify(&source_comp)
            .wait_timeout(std::time::Duration::from_millis(100))
            .expect("started");

        std::thread::sleep(std::time::Duration::from_millis(100));

        // The actual test:
        let client = async {
            let mut stream = TcpStream::connect(&addr).await.expect("couldn't connect");
            stream.write_all(b"2, 1").await.expect("write failed");
        };

        Runtime::new().unwrap().block_on(client);

        wait(1);
        sink_comp.on_definition(|cd| {
            assert_eq!(cd.data.len(), (1_usize));
            assert_eq!(cd.watermarks.last().unwrap().timestamp, 1);
        });
    }
}
