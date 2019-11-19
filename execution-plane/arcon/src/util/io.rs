extern crate tokio_codec;
use kompact::prelude::*;

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::codec::Decoder;
use tokio::net::TcpListener;
use tokio::net::{UdpFramed, UdpSocket};
use tokio::prelude::*;
use tokio::runtime::Runtime;
use tokio_codec::BytesCodec;

use std::thread::{Builder, JoinHandle};

use bytes::BytesMut;

pub enum IOKind {
    Http,
    Tcp,
    Udp,
}

#[derive(Debug)]
pub struct BytesRecv {
    pub bytes: BytesMut,
}
pub struct SockClosed {}
pub struct SockErr {}

#[derive(ComponentDefinition)]
pub struct IO {
    ctx: ComponentContext<IO>,
    _handle: JoinHandle<()>,
}

impl IO {
    pub fn udp(sock_addr: SocketAddr, subscriber: ActorRef<Box<dyn Any + Send>>) -> IO {
        let subscriber = Arc::new(subscriber);
        let th = Builder::new()
            .name(String::from("IOThread"))
            .spawn(move || {
                let mut runtime = Runtime::new().expect("Could not create Tokio Runtime!");
                let socket = UdpSocket::bind(&sock_addr).expect("Failed to bind");

                let (_, reader) = UdpFramed::new(socket, BytesCodec::new()).split();
                let handler = reader
                    .for_each(move |(bytes, _from)| {
                        let actor_ref = &*subscriber;
                        actor_ref.tell(Box::new(BytesRecv { bytes }) as Box<dyn Any + Send>);
                        Ok(())
                    })
                    .map_err(|_| ());

                runtime.spawn(handler);
                runtime.shutdown_on_idle().wait().unwrap();
            })
            .map_err(|_| ())
            .unwrap();

        IO {
            ctx: ComponentContext::new(),
            _handle: th,
        }
    }

    pub fn tcp(sock_addr: SocketAddr, subscriber: ActorRef<Box<dyn Any + Send>>) -> IO {
        let subscriber = Arc::new(subscriber);
        let th = Builder::new()
            .name(String::from("IOThread"))
            .spawn(move || {
                let mut runtime = Runtime::new().expect("Could not create Tokio Runtime!");
                let executor = runtime.executor();
                let listener = TcpListener::bind(&sock_addr).expect("failed to bind");
                let handler = listener
                    .incoming()
                    .map_err(|e| println!("failed to accept socket; error = {:?}", e))
                    .for_each(move |socket| {
                        let framed = BytesCodec::new().framed(socket);
                        let (_writer, reader) = framed.split();

                        let recv_sub = Arc::clone(&subscriber);
                        let close_sub = Arc::clone(&subscriber);
                        let err_sub = Arc::clone(&subscriber);

                        let processor = reader
                            .for_each(move |bytes| {
                                let tcp_recv = BytesRecv { bytes };
                                let actor_ref = &*recv_sub;
                                actor_ref.tell(Box::new(tcp_recv) as Box<dyn Any + Send>);
                                Ok(())
                            })
                            .and_then(move |()| {
                                let actor_ref = &*close_sub;
                                actor_ref.tell(Box::new(SockClosed {}) as Box<dyn Any + Send>);
                                Ok(())
                            })
                            .or_else(move |err| {
                                let actor_ref = &*err_sub;
                                actor_ref.tell(Box::new(SockErr {}) as Box<dyn Any + Send>);
                                Err(err)
                            })
                            .then(|_result| Ok(()));

                        executor.spawn(processor);
                        Ok(())
                    });

                runtime.spawn(handler);
                runtime.shutdown_on_idle().wait().unwrap();
            })
            .map_err(|_| ())
            .unwrap();

        IO {
            ctx: ComponentContext::new(),
            _handle: th,
        }
    }
}

//unsafe impl Send for IO {} // it is send anyway
//unsafe impl Sync for IO {} // it's not sync, but: does it need to be?

impl Provide<ControlPort> for IO {
    fn handle(&mut self, _event: ControlEvent) {}
}

impl Actor for IO {
    type Message = Box<dyn Any + Send>;
    fn receive_local(&mut self, _msg: Self::Message) {}
    fn receive_network(&mut self, _msg: NetMessage) {}
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use tokio::io;
    use tokio::net::TcpStream;

    pub enum IOKind {
        Tcp,
        Udp,
    }

    #[derive(ComponentDefinition)]
    pub struct IOSource {
        ctx: ComponentContext<IOSource>,
        kind: IOKind,
        sock_addr: SocketAddr,
        pub received: u64,
    }

    impl IOSource {
        pub fn new(sock_addr: SocketAddr, kind: IOKind) -> IOSource {
            IOSource {
                ctx: ComponentContext::new(),
                kind,
                sock_addr,
                received: 0,
            }
        }
    }

    impl Actor for IOSource {
        type Message = Box<dyn Any + Send>;

        fn receive_local(&mut self, msg: Self::Message) {
            if let Some(ref recv) = msg.downcast_ref::<BytesRecv>() {
                debug!(self.ctx.log(), "{:?}", recv.bytes);
                self.received += 1;
            } else if let Some(ref _close) = msg.downcast_ref::<SockClosed>() {
                debug!(self.ctx.log(), "Sock connection closed");
            } else if let Some(ref _err) = msg.downcast_ref::<SockErr>() {
                error!(self.ctx.log(), " Sock IO Error");
            }
        }

        fn receive_network(&mut self, _msg: NetMessage) {}
    }

    impl Provide<ControlPort> for IOSource {
        fn handle(&mut self, event: ControlEvent) {
            if let ControlEvent::Start = event {
                let system = self.ctx.system();
                match self.kind {
                    IOKind::Tcp => {
                        system.create_and_start(move || IO::tcp(self.sock_addr, self.actor_ref()));
                    }
                    IOKind::Udp => {
                        system.create_and_start(move || IO::udp(self.sock_addr, self.actor_ref()));
                    }
                }
            }
        }
    }

    #[test]
    fn tcp_io_test() {
        let system = KompactConfig::default().build().expect("KompactSystem");
        let addr = "127.0.0.1:3333".parse().unwrap();
        let (io_source, _) = system.create_and_register(move || IOSource::new(addr, IOKind::Tcp));
        system.start(&io_source);

        // Make sure IO::TCP is started
        std::thread::sleep(std::time::Duration::from_millis(100));

        let client = TcpStream::connect(&addr)
            .and_then(|stream| io::write_all(stream, "hello\n").then(|_| Ok(())))
            .map_err(|_| {
                assert!(false);
            });
        tokio::run(client);
        std::thread::sleep(std::time::Duration::from_millis(50));

        // Our IOSource should have received 1 msg, that is, "hello"
        let source = io_source.definition().lock().unwrap();
        assert_eq!(source.received, 1);
    }

    #[test]
    fn udp_io_test() {
        let system = KompactConfig::default().build().expect("KompactSystem");
        let sock = "127.0.0.1:9313".parse().unwrap();
        let (io_source, _) = system.create_and_register(move || IOSource::new(sock, IOKind::Udp));
        system.start(&io_source);
        std::thread::sleep(std::time::Duration::from_millis(100));

        let self_addr = "0.0.0.0:0".parse().unwrap();
        let socket = UdpSocket::bind(&self_addr).expect("Failed to bind");

        let fmt_data = format!("{:?}\n", "test1");
        let bytes = bytes::Bytes::from(fmt_data);
        socket.send_dgram(bytes, &sock).wait().unwrap();

        std::thread::sleep(std::time::Duration::from_millis(100));
        let source = io_source.definition().lock().unwrap();
        assert_eq!(source.received, 1);
    }

    #[allow(dead_code)]
    fn assert_io_send_and_sync(io: &IO) {
        fn assert_send<T: Send>(_t: &T) {}
        fn assert_sync<T: Sync>(_t: &T) {}

        assert_send(io);
        // TODO: Q: do we need IO to be Sync?
        //        assert_sync(io);
    }
}
