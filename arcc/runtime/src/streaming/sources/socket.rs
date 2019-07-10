use crate::tokio::prelude::Future;
use crate::util::io::*;
use bytes::BytesMut;
use kompact::*;
use std::str::from_utf8;
use std::str::FromStr;
use std::sync::Arc;
use tokio::io;
use tokio::net::TcpStream;
/*
    SocketSource:
    Generates events of type A from bytes received by a socket

*/

pub struct SocketSource<A: 'static + Send + Sync + Clone + FromStr> {
    ctx: ComponentContext<SocketSource<A>>,
    subscriber: Arc<ActorRef>,
    port: usize,
    received: u8,
}

impl<A: Send + Sync + Clone + FromStr> SocketSource<A> {
    pub fn new(port: usize, subscriber: ActorRef) -> SocketSource<A> {
        SocketSource {
            ctx: ComponentContext::new(),
            subscriber: Arc::new(subscriber),
            port: port,
            received: 0,
        }
    }
}

impl<A: Send + Sync + Clone + FromStr> Provide<ControlPort> for SocketSource<A> {
    fn handle(&mut self, event: ControlEvent) -> () {
        match event {
            ControlEvent::Start => {
                let port = self.port.clone();
                let system = self.ctx.system();
                let server =
                    system.create_and_start(move || IO::new(port, self.actor_ref(), IOKind::Tcp));
            }
            _ => {
                error!(self.ctx.log(), "bad ControlEvent");
            }
        }
    }
}

impl<A: Send + Sync + Clone + FromStr> ComponentDefinition for SocketSource<A> {
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
        "SocketSource"
    }
}

impl<A: Send + Sync + Clone + FromStr> Actor for SocketSource<A> {
    fn receive_local(&mut self, sender: ActorRef, msg: &Any) {
        info!(self.ctx.log(), "RECEIVED ANY");
        if let Some(ref recv) = msg.downcast_ref::<TcpRecv>() {
            info!(self.ctx.log(), "{:?}", recv.bytes);
            // Try to cast into our type from bytes
            if let Ok(byte_string) = from_utf8(&recv.bytes) {
                if let Ok(element) = byte_string.parse::<A>() {
                    self.received += 1;
                    self.subscriber.tell(Box::new(element), &self.actor_ref());
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
    use kompact::default_components::DeadletterBox;
    use std::{thread, time};

    // Stub for window-results
    mod sink {
        use super::*;

        pub struct Sink<A: 'static + Send + Clone> {
            ctx: ComponentContext<Sink<A>>,
            pub result: Vec<A>,
        }
        impl<A: Send + Clone> Sink<A> {
            pub fn new(_t: A) -> Sink<A> {
                Sink {
                    ctx: ComponentContext::new(),
                    result: Vec::new(),
                }
            }
        }
        impl<A: Send + Clone> Provide<ControlPort> for Sink<A> {
            fn handle(&mut self, _event: ControlEvent) -> () {}
        }
        impl<A: Send + Clone> Actor for Sink<A> {
            fn receive_local(&mut self, _sender: ActorRef, msg: &Any) {
                println!("sink received message");
                if let Some(m) = msg.downcast_ref::<A>() {
                    println!("trying to push");
                    self.result.push((*m).clone());
                } else {
                    println!("unrecognized message");
                }
            }
            fn receive_message(&mut self, _sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {}
        }
        impl<A: Send + Clone> ComponentDefinition for Sink<A> {
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
    fn test_setup<A: Send + Clone>(
        a: A,
    ) -> (
        kompact::KompactSystem,
        Arc<kompact::Component<sink::Sink<A>>>,
    ) {
        // Kompact set-up
        let system = KompactConfig::default().build().expect("KompactSystem");

        let (sink, _) = system.create_and_register(move || sink::Sink::new(a));

        system.start(&sink);

        return (system, sink);
    }
    // Test cases
    #[test]
    fn socket_u8() -> Result<(), Box<std::error::Error>> {
        let (system, sink) = test_setup(1 as u8);

        let file_source: SocketSource<u8> = SocketSource::new(3000, sink.actor_ref());
        let (source, _) = system.create_and_register(move || file_source);
        system.start(&source);

        wait(1);

        let addr = "127.0.0.1:3000".parse()?;
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
        assert_eq!(r0, 77 as u8);
        Ok(())
    }
    #[test]
    fn socket_string_with_newline() -> Result<(), Box<std::error::Error>> {
        let (system, sink) = test_setup(String::new());

        let file_source: SocketSource<String> = SocketSource::new(3000, sink.actor_ref());
        let (source, _) = system.create_and_register(move || file_source);
        system.start(&source);

        wait(1);

        let addr = "127.0.0.1:3000".parse()?;
        let client = TcpStream::connect(&addr)
            .and_then(|stream| io::write_all(stream, "the quick \nbrown fox").then(|_| Ok(())))
            .map_err(|_| {
                assert!(false);
            });

        tokio::run(client);

        wait(1);

        let source_inspect = source.definition().lock().unwrap();
        assert_eq!(source_inspect.received, 1);
        let sink_inspect = sink.definition().lock().unwrap();
        assert_eq!(sink_inspect.result.len(), (1 as usize));
        let r0 = &sink_inspect.result[0];
        assert_eq!(r0, &"the quick \nbrown fox".to_string());
        Ok(())
    }
    #[test]
    fn socket_multiple_f32() -> Result<(), Box<std::error::Error>> {
        let (system, sink) = test_setup(1 as f32);

        let file_source: SocketSource<f32> = SocketSource::new(3000, sink.actor_ref());
        let (source, _) = system.create_and_register(move || file_source);
        system.start(&source);

        wait(1);

        let addr = "127.0.0.1:3000".parse()?;
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
        assert_eq!(r0, 123 as f32);
        assert_eq!(r1, 4.56 as f32);
        assert_eq!(r2, 78.9 as f32);

        Ok(())
    }
}