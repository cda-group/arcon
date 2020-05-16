// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::{
    state,
    stream::{operator::Operator, source::SourceContext},
    timer::TimerBackend,
    util::io::*,
};
use kompact::prelude::*;
use std::{
    net::SocketAddr,
    str::{from_utf8, FromStr},
    time::Duration,
};

pub enum SocketKind {
    Tcp,
    Udp,
}

#[derive(ComponentDefinition)]
pub struct SocketSource<OP, B, T>
where
    OP: Operator<B> + 'static,
    OP::IN: FromStr,
    B: state::Backend,
    T: TimerBackend<OP::TimerState>,
{
    ctx: ComponentContext<Self>,
    source_ctx: SourceContext<OP, B, T>,
    sock_addr: SocketAddr,
    sock_kind: SocketKind,
}

impl<OP, B, T> SocketSource<OP, B, T>
where
    OP: Operator<B> + 'static,
    OP::IN: FromStr,
    B: state::Backend,
    T: TimerBackend<OP::TimerState>,
{
    pub fn new(
        sock_addr: SocketAddr,
        sock_kind: SocketKind,
        source_ctx: SourceContext<OP, B, T>,
    ) -> Self {
        assert!(source_ctx.watermark_interval > 0);
        SocketSource {
            ctx: ComponentContext::new(),
            source_ctx,
            sock_addr,
            sock_kind,
        }
    }
}

impl<OP, B, T> Provide<ControlPort> for SocketSource<OP, B, T>
where
    OP: Operator<B> + 'static,
    OP::IN: FromStr,
    B: state::Backend,
    T: TimerBackend<OP::TimerState>,
{
    fn handle(&mut self, event: ControlEvent) {
        if let ControlEvent::Start = event {
            let system = self.ctx.system();

            // Schedule periodic watermark generation
            self.schedule_periodic(
                Duration::from_secs(0),
                Duration::from_secs(self.source_ctx.watermark_interval),
                move |self_c, _| {
                    self_c.source_ctx.generate_watermark();
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
        }
    }
}

impl<OP, B, T> Actor for SocketSource<OP, B, T>
where
    OP: Operator<B> + 'static,
    OP::IN: FromStr,
    B: state::Backend,
    T: TimerBackend<OP::TimerState>,
{
    type Message = IOMessage;

    fn receive_local(&mut self, msg: Self::Message) {
        match msg {
            IOMessage::Bytes(bytes) => {
                debug!(self.ctx.log(), "{:?}", bytes);
                if let Ok(byte_string) = from_utf8(&bytes) {
                    if let Ok(in_data) = byte_string.trim().parse::<OP::IN>() {
                        let elem = self.source_ctx.extract_element(in_data);
                        self.source_ctx.process(elem);
                    } else {
                        error!(self.ctx.log(), "Unable to parse string {}", byte_string);
                    }
                }
            }
            IOMessage::SockClosed => info!(self.ctx.log(), "Sock connection closed"),
            IOMessage::SockErr => error!(self.ctx.log(), "Sock IO Error"),
        }
    }

    fn receive_network(&mut self, _msg: NetMessage) {
        error!(self.ctx.log(), "Got unexpected message");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        pipeline::ArconPipeline,
        prelude::{Channel, ChannelStrategy, DebugNode, Forward, Map},
        state::{Backend, InMemory},
        timer,
    };
    use std::{thread, time};
    use tokio::{net::TcpStream, prelude::*, runtime::Runtime};

    // Shared methods for test cases
    fn wait(time: u64) -> () {
        thread::sleep(time::Duration::from_secs(time));
    }

    // Test cases
    #[test]
    fn socket_u32_test() {
        // Setup conf
        let addr = "127.0.0.1:4000".parse().unwrap();

        // Setup
        let mut pipeline = ArconPipeline::new();
        let pool_info = pipeline.get_pool_info();
        let system = pipeline.system();

        let (sink, _) = system.create_and_register(move || DebugNode::<u32>::new());
        let sink_ref = sink.actor_ref().hold().expect("Failed to fetch strong ref");

        let channel = Channel::Local(sink_ref);
        let channel_strategy = ChannelStrategy::Forward(Forward::new(channel, 1.into(), pool_info));

        // just pass it on
        fn map_fn(x: u32) -> u32 {
            x
        }

        // Set up SourceContext
        let watermark_interval = 1; // in seconds currently for SocketSource

        let source_context = SourceContext::new(
            watermark_interval,
            None, // no timestamp extractor
            channel_strategy,
            Map::new(&map_fn),
            InMemory::create("test".as_ref()).unwrap(),
            timer::none(),
        );

        let socket_source = SocketSource::new(addr, SocketKind::Tcp, source_context);
        let (source, _) = system.create_and_register(move || socket_source);

        system.start(&sink);
        system.start(&source);
        wait(1);

        // The actual test:
        let client = async {
            let mut stream = TcpStream::connect(&addr).await.expect("couldn't connect");
            stream.write_all(b"77").await.expect("write failed");
        };

        Runtime::new().unwrap().block_on(client);

        wait(1);
        let sink_inspect = sink.definition().lock().unwrap();
        assert_eq!(sink_inspect.data.len(), (1 as usize));
        let r0 = &sink_inspect.data[0];
        assert_eq!(r0.data, 77);
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
        let mut pipeline = ArconPipeline::new();
        let pool_info = pipeline.get_pool_info();
        let system = pipeline.system();

        let (sink, _) = system.create_and_register(DebugNode::<ExtractorStruct>::new);
        let sink_ref = sink.actor_ref().hold().expect("Failed to fetch strong ref");
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

        let source_context = SourceContext::new(
            watermark_interval,
            Some(&timestamp_extractor),
            channel_strategy,
            Map::new(&map_fn),
            InMemory::create("test".as_ref()).unwrap(),
            timer::none(),
        );

        let socket_source = SocketSource::new(addr, SocketKind::Tcp, source_context);
        let (source, _) = system.create_and_register(move || socket_source);

        system.start(&sink);
        system.start(&source);
        wait(1);

        // The actual test:
        let client = async {
            let mut stream = TcpStream::connect(&addr).await.expect("couldn't connect");
            stream.write_all(b"2, 1").await.expect("write failed");
        };

        Runtime::new().unwrap().block_on(client);

        wait(1);
        let sink_inspect = sink.definition().lock().unwrap();
        assert_eq!(sink_inspect.data.len(), (1 as usize));
        assert_eq!(sink_inspect.watermarks.last().unwrap().timestamp, 1);
    }
}
