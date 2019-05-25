use grpc::arcc::*;
use grpc::prelude::*;

use grpc::arcc_grpc::create_compiler;
use grpc::arcc_grpc::Compiler;
use std::io::Read;
use std::sync::Arc;
use std::{io, thread};

use spec::ArcSpec;

#[derive(Clone)]
struct CompilerService;

impl Compiler for CompilerService {
    fn compile(&mut self, ctx: RpcContext, req: CompileRequest, sink: UnarySink<CompileReply>) {
        let spec = ArcSpec::from_bytes(&req.spec);
        info!("Received Compilation Request {:?}", spec);
    }
}

pub fn start_server(host: &str, port: i32) {
    let env = Arc::new(Environment::new(1));
    let service = create_compiler(CompilerService);
    let mut server = ServerBuilder::new(env)
        .register_service(service)
        .bind(host, port as u16)
        .build()
        .unwrap();
    server.start();

    for &(ref host, port) in server.bind_addrs() {
        println!("listening on {}:{}", host, port);
    }
    let (tx, rx) = oneshot::channel();
    thread::spawn(move || {
        println!("Press ENTER to exit...");
        let _ = io::stdin().read(&mut [0]).unwrap();
        tx.send(())
    });
    let _ = rx.wait();
    let _ = server.shutdown().wait();
}
