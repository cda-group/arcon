use crate::data::{ArconElement, ArconEvent, ArconType, Watermark};
use crate::streaming::channel::strategy::ChannelStrategy;
use crate::util::io::*;
use kompact::*;
use std::str::from_utf8;
use std::str::FromStr;
use std::time::Duration;
use std::time::SystemTime;

/*
    SocketSource:
    Generates events of type OUT from bytes received by a socket
    Attempts to cast the bytes as string to type OUT
    Causes Error! if it's unable to cast from string
    If the watermark_interval argument is n>0 it will output watermarks every n seconds
    AND it will add a timestamp of the ingestion time to each event outputted
*/
#[derive(ComponentDefinition)]
pub struct SocketSource<OUT>
where
    OUT: 'static + ArconType + FromStr,
{
    ctx: ComponentContext<SocketSource<OUT>>,
    out_channels: Box<ChannelStrategy<OUT>>,
    tcp_port: usize,
    received: u8,
    watermark_interval: u64, // If 0: no watermarks/timestamps generated
}

impl<OUT> SocketSource<OUT>
where
    OUT: 'static + ArconType + FromStr,
{
    pub fn new(
        tcp_port: usize,
        out_channels: Box<ChannelStrategy<OUT>>,
        watermark_interval: u64,
    ) -> SocketSource<OUT> {
        SocketSource {
            ctx: ComponentContext::new(),
            out_channels,
            tcp_port,
            watermark_interval,
            received: 0,
        }
    }
    pub fn output_event(&mut self, data: OUT) -> () {
        self.received += 1;
        if self.watermark_interval > 0 {
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
        } else {
            if let Err(err) = self.out_channels.output(
                ArconEvent::Element(ArconElement::new(data)),
                &self.ctx.system()
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
                let tcp_port = self.tcp_port.clone();
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
                let _server = system
                    .create_and_start(move || IO::new(tcp_port, self.actor_ref(), IOKind::Tcp));
            }
            _ => {
                error!(self.ctx.log(), "bad ControlEvent");
            }
        }
    }
}

impl<OUT> Actor for SocketSource<OUT>
where
    OUT: 'static + ArconType + FromStr,
{
    fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
        if let Some(ref recv) = msg.downcast_ref::<TcpRecv>() {
            debug!(self.ctx.log(), "{:?}", recv.bytes);
            // Try to cast into our type from bytes
            if let Ok(byte_string) = from_utf8(&recv.bytes) {
                if let Ok(data) = byte_string.trim().parse::<OUT>() {
                    self.output_event(data);
                } else {
                    error!(self.ctx.log(), "Unable to parse string {}", byte_string);
                }
            } else {
                error!(self.ctx.log(), "Unable to parse bytes to string");
            }
        } else if let Some(ref _close) = msg.downcast_ref::<TcpClosed>() {
            info!(self.ctx.log(), "TCP connection closed");
        } else if let Some(ref _err) = msg.downcast_ref::<TcpErr>() {
            error!(self.ctx.log(), "TCP IO Error");
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

    // Stub for window-results
    mod sink {
        use super::*;
        use crate::data::ArconEvent::Element;
        use crate::data::Watermark;
        use std::sync::Arc;

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
        impl<A: ArconType> ComponentDefinition for Sink<A> {
            fn setup(&mut self, self_component: Arc<Component<Self>>) -> () {
                self.ctx_mut().initialise(self_component);
            }
            fn execute(&mut self, _max_events: usize, skip: usize) -> ExecuteResult {
                ExecuteResult::new(skip, skip)
            }
            fn ctx(&self) -> &ComponentContext<Self> {
                &self.ctx
            }
            fn ctx_mut(&mut self) -> &mut ComponentContext<Self> {
                &mut self.ctx
            }
            fn type_name() -> &'static str {
                "EventTimeWindowAssigner"
            }
        }
    }
    // Shared methods for test cases
    fn wait(time: u64) -> () {
        thread::sleep(time::Duration::from_secs(time));
    }
    // Test cases
    #[test]
    fn socket_u8_no_watermark() -> Result<(), Box<std::error::Error>> {
        // Setup conf
        let port = 4000;

        // Setup
        let system = KompactConfig::default().build().expect("KompactSystem");

        let (sink, _) = system.create_and_register(move || sink::Sink::<u8>::new());
        let sink_ref = sink.actor_ref();

        let out_channels: Box<Forward<u8>> =
            Box::new(Forward::new(Channel::Local(sink_ref.clone())));

        let socket_source: SocketSource<u8> = SocketSource::new(port, out_channels, 0);
        let (source, _) = system.create_and_register(move || socket_source);

        system.start(&sink);
        system.start(&source);
        wait(1);
        let addr = ("127.0.0.1:".to_string() + &port.to_string()).parse()?;

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
        Ok(())
    }

    #[test]
    fn socket_multiple_f32_no_watermark() -> Result<(), Box<std::error::Error>> {
        // Setup conf
        let port = 4001;

        // Setup
        let system = KompactConfig::default().build().expect("KompactSystem");

        let (sink, _) = system.create_and_register(move || sink::Sink::<f32>::new());
        let sink_ref = sink.actor_ref();

        let out_channels: Box<Forward<f32>> =
            Box::new(Forward::new(Channel::Local(sink_ref.clone())));

        let socket_source: SocketSource<f32> = SocketSource::new(port, out_channels, 0);
        let (source, _) = system.create_and_register(move || socket_source);

        system.start(&sink);
        system.start(&source);
        wait(1);
        let addr = ("127.0.0.1:".to_string() + &port.to_string()).parse()?;

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

        Ok(())
    }
    #[test]
    fn socket_u8_with_watermark() -> Result<(), Box<std::error::Error>> {
        // Setup conf
        let port = 4002;

        // Setup
        let system = KompactConfig::default().build().expect("KompactSystem");

        let (sink, _) = system.create_and_register(move || sink::Sink::<u8>::new());
        let sink_ref = sink.actor_ref();

        let out_channels: Box<Forward<u8>> =
            Box::new(Forward::new(Channel::Local(sink_ref.clone())));

        let socket_source: SocketSource<u8> = SocketSource::new(port, out_channels, 3);
        let (source, _) = system.create_and_register(move || socket_source);

        system.start(&sink);
        system.start(&source);
        wait(1);
        let addr = ("127.0.0.1:".to_string() + &port.to_string()).parse()?;

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
        Ok(())
    }
}
