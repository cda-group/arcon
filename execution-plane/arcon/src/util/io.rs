// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use kompact::prelude::*;

use futures::StreamExt;
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    net::{TcpListener, UdpSocket},
    runtime::Runtime,
};
use tokio_util::{
    codec::{BytesCodec, Decoder},
    udp::UdpFramed,
};

use bytes::BytesMut;
use std::thread::{Builder, JoinHandle};

/// Events that an IO Component may send to its subscriber
#[derive(Debug)]
pub enum IOMessage {
    /// Some raw bytes packed into a BytesMut
    Bytes(BytesMut),
    /// Indicates that the socket connection closed
    SockClosed,
    /// Indicates that an error occured
    SockErr,
}

/// A [kompact] Component listening to IO
///
/// Supports both TCP and UDP.
#[derive(ComponentDefinition)]
pub struct IO {
    ctx: ComponentContext<IO>,
    _handle: JoinHandle<()>,
}

impl IO {
    /// Creates a UDP IO component
    pub fn udp(sock_addr: SocketAddr, subscriber: ActorRef<IOMessage>) -> IO {
        let th = Builder::new()
            .name(String::from("IOThread"))
            .spawn(move || {
                let mut runtime = Runtime::new().expect("Could not create Tokio Runtime!");

                runtime.block_on(async move {
                    let socket = UdpSocket::bind(&sock_addr).await.expect("Failed to bind");

                    let (_, mut reader) = UdpFramed::new(socket, BytesCodec::new()).split();

                    while let Some(Ok((bytes, _from))) = reader.next().await {
                        subscriber.tell(IOMessage::Bytes(bytes));
                    }
                });
            })
            .map_err(|_| ())
            .unwrap();

        IO {
            ctx: ComponentContext::new(),
            _handle: th,
        }
    }

    /// Creates a TCP IO component
    pub fn tcp(sock_addr: SocketAddr, subscriber: ActorRef<IOMessage>) -> IO {
        let th = Builder::new()
            .name(String::from("IOThread"))
            .spawn(move || {
                let mut runtime = Runtime::new().expect("Could not create Tokio Runtime!");
                let handle = runtime.handle().clone();
                runtime.block_on(async move {
                    let mut listener = TcpListener::bind(&sock_addr).await.expect("failed to bind");

                    let mut incoming = listener.incoming();
                    let subscriber = Arc::new(subscriber);
                    'incoming: while let Some(socket_res) = incoming.next().await {
                        let socket = match socket_res {
                            Ok(s) => s,
                            Err(_) => {
                                // TODO: logging?
                                continue 'incoming;
                            }
                        };

                        let framed = BytesCodec::new().framed(socket);
                        let (_writer, mut reader) = framed.split();

                        let subscriber = Arc::clone(&subscriber);
                        let processor = async move {
                            let mut res = Ok(());

                            while let Some(read_res) = reader.next().await {
                                match read_res {
                                    Ok(bytes) => {
                                        subscriber.tell(IOMessage::Bytes(bytes));
                                    }
                                    Err(e) => {
                                        res = Err(e);
                                    }
                                }
                            }

                            match res {
                                Ok(()) => {
                                    subscriber.tell(IOMessage::SockClosed);
                                }
                                Err(_err) => {
                                    subscriber.tell(IOMessage::SockErr);
                                }
                            }
                        };

                        handle.spawn(processor);
                    }
                });
            })
            .map_err(|_| ())
            .unwrap();

        IO {
            ctx: ComponentContext::new(),
            _handle: th,
        }
    }
}

impl Provide<ControlPort> for IO {
    fn handle(&mut self, _event: ControlEvent) {}
}

impl Actor for IO {
    type Message = ();
    fn receive_local(&mut self, _msg: Self::Message) {}
    fn receive_network(&mut self, _msg: NetMessage) {}
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use tokio::{io::AsyncWriteExt, net::TcpStream};

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
        type Message = IOMessage;

        fn receive_local(&mut self, msg: Self::Message) {
            match msg {
                IOMessage::Bytes(bytes) => {
                    debug!(self.ctx.log(), "{:?}", bytes);
                    self.received += 1;
                }
                IOMessage::SockClosed => {
                    debug!(self.ctx.log(), "Sock connection closed");
                }
                IOMessage::SockErr => {
                    error!(self.ctx.log(), " Sock IO Error");
                }
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
                        let comp = system.create(move || IO::tcp(self.sock_addr, self.actor_ref()));
                        system.start(&comp);
                    }
                    IOKind::Udp => {
                        let comp = system.create(move || IO::udp(self.sock_addr, self.actor_ref()));
                        system.start(&comp);
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

        let client = async {
            let mut stream = TcpStream::connect(&addr).await.expect("couldn't connect");
            stream.write_all(b"hello\n").await.expect("write failed");
        };

        Runtime::new().unwrap().block_on(client);
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

        let client = async {
            let self_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
            let mut socket = UdpSocket::bind(&self_addr).await.expect("Failed to bind");

            let fmt_data = format!("{:?}\n", "test1");
            let bytes = bytes::Bytes::from(fmt_data);
            socket.send_to(&bytes, &sock).await.unwrap();
        };

        Runtime::new().unwrap().block_on(client);

        std::thread::sleep(std::time::Duration::from_millis(100));
        let source = io_source.definition().lock().unwrap();
        assert_eq!(source.received, 1);
    }
}
