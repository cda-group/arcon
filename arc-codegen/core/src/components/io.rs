extern crate tokio_codec;
use kompact::prelude::*;
use kompact::*;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::codec::Decoder;
use tokio::net::TcpListener;
use tokio::prelude::*;
use tokio_codec::BytesCodec;

use bytes::BytesMut;
use futures::Future;

pub enum IOKind {
    Http,
    Tcp,
}

pub struct TcpRecv {
    pub bytes: BytesMut,
}

pub struct TcpClosed {}
pub struct TcpErr {}

pub struct HttpRecv {
    pub bytes: BytesMut,
}

#[derive(ComponentDefinition)]
pub struct IO {
    ctx: ComponentContext<IO>,
    port: usize,
    subscriber: Arc<ActorRef>,
    kind: IOKind,
}

impl IO {
    pub fn new(port: usize, subscriber: ActorRef, kind: IOKind) -> IO {
        IO {
            ctx: ComponentContext::new(),
            port,
            subscriber: Arc::new(subscriber),
            kind,
        }
    }

    fn tcp_server(&mut self) -> Result<(), Box<std::error::Error>> {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), self.port as u16);
        let listener = TcpListener::bind(&addr)?;
        let subscriber = Arc::clone(&self.subscriber);

        let server = listener
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
                        let tcp_recv = TcpRecv { bytes };
                        let actor_ref = &*recv_sub;
                        actor_ref.tell(Box::new(tcp_recv), actor_ref);
                        Ok(())
                    })
                    .and_then(move |()| {
                        let actor_ref = &*close_sub;
                        actor_ref.tell(Box::new(TcpClosed {}), actor_ref);
                        Ok(())
                    })
                    .or_else(move |err| {
                        let actor_ref = &*err_sub;
                        actor_ref.tell(Box::new(TcpErr {}), actor_ref);
                        Err(err)
                    })
                    .then(|_result| Ok(()));
                tokio::spawn(processor)
            });

        info!(
            self.ctx.log(),
            "Starting IO::TCP on localhost:{}", self.port
        );

        tokio::run(server);
        Ok(())
    }
}

unsafe impl Send for IO {}
unsafe impl Sync for IO {}

impl Provide<ControlPort> for IO {
    fn handle(&mut self, event: ControlEvent) {
        if let ControlEvent::Start = event {
            match &self.kind {
                IOKind::Tcp => {
                    let _ = self.tcp_server();
                }
                IOKind::Http => {
                    // Not implemented
                }
            }
        }
    }
}

impl Actor for IO {
    fn receive_local(&mut self, _sender: ActorRef, _msg: Box<Any>) {}
    fn receive_message(&mut self, sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {
        error!(self.ctx.log(), "Got unexpected message from {}", sender);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io;
    use tokio::net::TcpStream;
    use tokio::prelude::*;

    #[derive(ComponentDefinition)]
    pub struct TcpSource {
        ctx: ComponentContext<TcpSource>,
        pub received: u64,
    }

    impl TcpSource {
        pub fn new() -> TcpSource {
            TcpSource {
                ctx: ComponentContext::new(),
                received: 0,
            }
        }
    }

    impl Actor for TcpSource {
        fn receive_local(&mut self, _sender: ActorRef, msg: Box<Any>) {
            if let Some(ref recv) = msg.downcast_ref::<TcpRecv>() {
                info!(self.ctx.log(), "{:?}", recv.bytes);
                self.received += 1;
            } else if let Some(ref _close) = msg.downcast_ref::<TcpClosed>() {
                info!(self.ctx.log(), "TCP connection closed");
            } else if let Some(ref _err) = msg.downcast_ref::<TcpErr>() {
                error!(self.ctx.log(), "TCP IO Error");
            }
        }
        fn receive_message(&mut self, sender: ActorPath, _ser_id: u64, _buf: &mut Buf) {
            error!(self.ctx.log(), "Got unexpected message from {}", sender);
        }
    }

    impl Provide<ControlPort> for TcpSource {
        fn handle(&mut self, event: ControlEvent) {
            if let ControlEvent::Start = event {
                let port = 3000;
                let system = self.ctx.system();
                system.create_and_start(move || IO::new(port, self.actor_ref(), IOKind::Tcp));
            }
        }
    }

    #[test]
    fn tcp_io() -> Result<(), Box<std::error::Error>> {
        let system = KompactConfig::default().build().expect("KompactSystem");
        let (tcp_source, _t) = system.create_and_register(move || TcpSource::new());
        system.start(&tcp_source);

        // Make sure IO::TCP is started
        std::thread::sleep(std::time::Duration::from_millis(100));

        let addr = "127.0.0.1:3000".parse()?;
        let client = TcpStream::connect(&addr)
            .and_then(|stream| io::write_all(stream, "hello\n").then(|result| Ok(())))
            .map_err(|err| {
                assert!(false);
            });

        tokio::run(client);

        std::thread::sleep(std::time::Duration::from_millis(50));

        // Our TcpSource should have received 1 msg, that is, "hello"
        let source = tcp_source.definition().lock().unwrap();
        assert_eq!(source.received, 1);
        Ok(())
    }
}
