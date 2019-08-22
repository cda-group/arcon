use crate::data::{ArconElement, ArconEvent, ArconType, Watermark};
use crate::streaming::channel::strategy::ChannelStrategy;
use crate::util::io::*;
use kompact::*;
use std::net::SocketAddr;
use std::str::from_utf8;
use std::str::FromStr;
use std::time::Duration;
use std::time::SystemTime;

pub enum SocketKind {
    Tcp,
    Udp,
}

/// `SocketSource` Generates events of type `OUT` from bytes received by a socket
///
/// If the `watermark_interval` argument is n>0 it will output watermarks every n seconds
/// AND it will add a timestamp of the ingestion time to each event outputted
#[derive(ComponentDefinition)]
pub struct SocketSource<OUT>
where
    OUT: 'static + ArconType + FromStr,
{
    ctx: ComponentContext<SocketSource<OUT>>,
    out_channels: Box<ChannelStrategy<OUT>>,
    sock_addr: SocketAddr,
    sock_kind: SocketKind,
    received: u8,
    watermark_interval: u64, // If 0: no watermarks/timestamps generated
    watermark_index: Option<u32>,
}

impl<OUT> SocketSource<OUT>
where
    OUT: 'static + ArconType + FromStr,
{
    pub fn new(
        sock_addr: SocketAddr,
        sock_kind: SocketKind,
        out_channels: Box<ChannelStrategy<OUT>>,
        watermark_interval: u64,
        watermark_index: Option<u32>,
    ) -> SocketSource<OUT> {
        SocketSource {
            ctx: ComponentContext::new(),
            out_channels,
            sock_addr,
            sock_kind,
            watermark_interval,
            watermark_index,
            received: 0,
        }
    }
    pub fn output_event(&mut self, data: OUT, ts: Option<u64>) -> () {
        self.received += 1;
        if self.watermark_interval > 0 {
            if let Some(timestamp) = ts {
                debug!(self.ctx.log(), "Extracted timestamp and using that");
                if let Err(err) = self.out_channels.output(
                    ArconEvent::Element(ArconElement::with_timestamp(data, timestamp)),
                    &self.ctx.system(),
                ) {
                    error!(self.ctx.log(), "Unable to output event, error {}", err);
                }
            } else {
                // This should be replaced with a Timestamp extractor, we use ingestiontime for now
                match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
                    Ok(ts) => {
                        if let Err(err) = self.out_channels.output(
                            ArconEvent::Element(ArconElement::with_timestamp(data, ts.as_secs())),
                            &self.ctx.system(),
                        ) {
                            error!(self.ctx.log(), "Unable to output event, error {}", err);
                        }
                    }
                    _ => {
                        error!(self.ctx.log(), "Failed to read SystemTime");
                    }
                }
            }
        } else {
            if let Err(err) = self.out_channels.output(
                ArconEvent::Element(ArconElement::new(data)),
                &self.ctx.system(),
            ) {
                error!(self.ctx.log(), "Unable to output event, error {}", err);
            }
        }
    }
    pub fn output_watermark(&mut self) -> () {
        match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(n) => {
                if let Err(err) = self.out_channels.output(
                    ArconEvent::Watermark(Watermark::new(n.as_secs())),
                    &self.ctx.system(),
                ) {
                    error!(self.ctx.log(), "Unable to output watermark, error {}", err);
                }
            }
            _ => {
                error!(self.ctx.log(), "Failed to read SystemTime");
            }
        }
    }
}

impl<OUT> Provide<ControlPort> for SocketSource<OUT>
where
    OUT: 'static + ArconType + FromStr,
{
    fn handle(&mut self, event: ControlEvent) -> () {
        match event {
            ControlEvent::Start => {
                let system = self.ctx.system();

                // Check if we should schedule watermark generation
                if self.watermark_interval > 0 {
                    self.schedule_periodic(
                        Duration::from_secs(0),
                        Duration::from_secs(self.watermark_interval),
                        move |self_c, _| {
                            self_c.output_watermark();
                        },
                    );
                }
                match self.sock_kind {
                    SocketKind::Tcp => {
                        let _ = system
                            .create_and_start(move || IO::tcp(self.sock_addr, self.actor_ref()));
                    }
                    SocketKind::Udp => {
                        let _ = system
                            .create_and_start(move || IO::udp(self.sock_addr, self.actor_ref()));
                    }
                }
            }
            _ => {}
        }
    }
}

impl<OUT> Actor for SocketSource<OUT>
where
    OUT: 'static + ArconType + FromStr,
{
    fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
        if let Some(ref recv) = msg.downcast_ref::<BytesRecv>() {
            debug!(self.ctx.log(), "{:?}", recv.bytes);
            // Try to cast into our type from bytes
            if let Ok(byte_string) = from_utf8(&recv.bytes) {
                // NOTE: Hacky...
                if let Some(wm_index) = self.watermark_index {
                    // Just assume it is at first place
                    let v: Vec<String> = byte_string
                        .trim()
                        .split(",")
                        .collect::<Vec<&str>>()
                        .iter()
                        .map(|s| s.trim().to_string())
                        .collect();
                    if v.is_empty() {
                        error!(
                            self.ctx.log(),
                            "Bad input data, should be delimited by comma"
                        );
                    } else {
                        if let Some(ts_str) = v.get(wm_index as usize) {
                            if let Ok(ts) =  ts_str.parse::<u64>() {
                                let input_data = v.join(",");
                                debug!(self.ctx.log(), "Trying to parse str {}", input_data);
                                match input_data.parse::<OUT>() {
                                    Ok(data) => self.output_event(data, Some(ts)),
                                    Err(_) => error!(self.ctx.log(), "Unable to parse string {:?}", input_data),
                                }
                            } else {
                                error!(self.ctx.log(), "Failed to extract timestamp at index {}", wm_index);
                            }
                        }
                    }
                } else {
                    if let Ok(data) = byte_string.trim().parse::<OUT>() {
                        self.output_event(data, None);
                    } else {
                        error!(self.ctx.log(), "Unable to parse string {}", byte_string);
                    }
                }
            } else {
                error!(self.ctx.log(), "Unable to parse bytes to string");
            }
        } else if let Some(ref _close) = msg.downcast_ref::<SockClosed>() {
            info!(self.ctx.log(), "Sock connection closed");
        } else if let Some(ref _err) = msg.downcast_ref::<SockErr>() {
            error!(self.ctx.log(), "Sock IO Error");
        } else {
            error!(self.ctx.log(), "Unrecognized Message");
        }
    }
    fn receive_message(&mut self, sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {
        error!(self.ctx.log(), "Got unexpected message from {}", sender);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::channel::strategy::forward::Forward;
    use crate::streaming::channel::Channel;
    use crate::tokio::prelude::Future;
    use std::{thread, time};
    use tokio::io;
    use tokio::net::TcpStream;

    mod sink {
        use super::*;
        use crate::data::ArconEvent::Element;
        use crate::data::Watermark;

        #[derive(ComponentDefinition)]
        pub struct Sink<A: 'static + ArconType> {
            ctx: ComponentContext<Sink<A>>,
            pub result: Vec<ArconElement<A>>,
            pub watermarks: Vec<Watermark>,
        }
        impl<A: ArconType> Sink<A> {
            pub fn new() -> Sink<A> {
                Sink {
                    ctx: ComponentContext::new(),
                    result: Vec::new(),
                    watermarks: Vec::new(),
                }
            }
        }
        impl<A: ArconType> Provide<ControlPort> for Sink<A> {
            fn handle(&mut self, _event: ControlEvent) -> () {}
        }
        impl<A: ArconType> Actor for Sink<A> {
            fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
                if let Some(event) = msg.downcast_ref::<ArconEvent<A>>() {
                    match event {
                        Element(e) => {
                            self.result.push(*e);
                        }
                        ArconEvent::Watermark(w) => {
                            self.watermarks.push(*w);
                        }
                    }
                } else {
                }
            }
            fn receive_message(&mut self, _sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {}
        }
    }
    // Shared methods for test cases
    fn wait(time: u64) -> () {
        thread::sleep(time::Duration::from_secs(time));
    }
    // Test cases
    #[test]
    fn socket_u8_no_watermark() {
        // Setup conf
        let addr = "127.0.0.1:4000".parse().unwrap();

        // Setup
        let system = KompactConfig::default().build().expect("KompactSystem");

        let (sink, _) = system.create_and_register(move || sink::Sink::<u8>::new());
        let sink_ref = sink.actor_ref();

        let out_channels: Box<Forward<u8>> =
            Box::new(Forward::new(Channel::Local(sink_ref.clone())));

        let socket_source: SocketSource<u8> =
            SocketSource::new(addr, SocketKind::Tcp, out_channels, 0, None);
        let (source, _) = system.create_and_register(move || socket_source);

        system.start(&sink);
        system.start(&source);
        wait(1);

        // The actual test:
        let client = TcpStream::connect(&addr)
            .and_then(|stream| io::write_all(stream, "77").then(|_| Ok(())))
            .map_err(|_| {
                assert!(false);
            });
        tokio::run(client);

        wait(1);
        let source_inspect = source.definition().lock().unwrap();
        assert_eq!(source_inspect.received, 1);
        let sink_inspect = sink.definition().lock().unwrap();
        assert_eq!(sink_inspect.result.len(), (1 as usize));
        let r0 = sink_inspect.result[0];
        assert_eq!(r0.data, 77 as u8);
    }

    #[test]
    fn socket_multiple_f32_no_watermark() {
        // Setup conf
        let addr = "127.0.0.1:4001".parse().unwrap();

        // Setup
        let system = KompactConfig::default().build().expect("KompactSystem");

        let (sink, _) = system.create_and_register(move || sink::Sink::<f32>::new());
        let sink_ref = sink.actor_ref();

        let out_channels: Box<Forward<f32>> =
            Box::new(Forward::new(Channel::Local(sink_ref.clone())));

        let socket_source: SocketSource<f32> =
            SocketSource::new(addr, SocketKind::Tcp, out_channels, 0, None);
        let (source, _) = system.create_and_register(move || socket_source);

        system.start(&sink);
        system.start(&source);
        wait(1);

        // The actual test:
        let client1 = TcpStream::connect(&addr)
            .and_then(|stream| io::write_all(stream, "123").then(|_| Ok(())))
            .map_err(|_| {
                assert!(false);
            });
        let client2 = TcpStream::connect(&addr)
            .and_then(|stream| io::write_all(stream, "4.56").then(|_| Ok(())))
            .map_err(|_| {
                assert!(false);
            });
        let client3 = TcpStream::connect(&addr)
            .and_then(|stream| io::write_all(stream, "78.9").then(|_| Ok(())))
            .map_err(|_| {
                assert!(false);
            });
        tokio::run(client1);
        tokio::run(client2);
        tokio::run(client3);

        wait(1);

        let source_inspect = source.definition().lock().unwrap();
        assert_eq!(source_inspect.received, 3);
        let sink_inspect = sink.definition().lock().unwrap();
        assert_eq!(sink_inspect.result.len(), (3 as usize));
        let r0 = sink_inspect.result[0];
        let r1 = sink_inspect.result[1];
        let r2 = sink_inspect.result[2];
        assert_eq!(r0.data, 123 as f32);
        assert_eq!(r1.data, 4.56 as f32);
        assert_eq!(r2.data, 78.9 as f32);
    }
    #[test]
    fn socket_u8_with_watermark() {
        // Setup conf
        let addr = "127.0.0.1:4002".parse().unwrap();

        // Setup
        let system = KompactConfig::default().build().expect("KompactSystem");

        let (sink, _) = system.create_and_register(move || sink::Sink::<u8>::new());
        let sink_ref = sink.actor_ref();

        let out_channels: Box<Forward<u8>> =
            Box::new(Forward::new(Channel::Local(sink_ref.clone())));

        let socket_source: SocketSource<u8> =
            SocketSource::new(addr, SocketKind::Tcp, out_channels, 3, None);
        let (source, _) = system.create_and_register(move || socket_source);

        system.start(&sink);
        system.start(&source);
        wait(1);

        // The actual test:
        let client = TcpStream::connect(&addr)
            .and_then(|stream| io::write_all(stream, "77").then(|_| Ok(())))
            .map_err(|_| {
                assert!(false);
            });
        tokio::run(client);

        wait(3);
        let source_inspect = source.definition().lock().unwrap();
        assert_eq!(source_inspect.received, 1);
        let sink_inspect = sink.definition().lock().unwrap();
        assert_eq!(sink_inspect.result.len(), (1 as usize));
        let r0 = sink_inspect.result[0];
        assert_eq!(r0.data, 77 as u8);
        assert_ne!(r0.timestamp, None); // Check that the timestamp is not None
        assert_eq!(sink_inspect.watermarks.len(), (2 as usize));
    }
}
