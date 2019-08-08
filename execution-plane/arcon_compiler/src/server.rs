use grpc::arconc::*;
use grpc::prelude::{Environment, RpcContext, ServerBuilder, UnarySink};

use grpc::arconc_grpc::create_arconc;
use grpc::arconc_grpc::Arconc;
use std::sync::{Arc, Mutex};

use crate::env::CompilerEnv;
use arcon_spec::ArcSpec;

#[derive(Clone)]
struct Server {
    env: Arc<Mutex<CompilerEnv>>,
}

impl Server {
    pub fn new(env: CompilerEnv) -> Server {
        Server {
            env: Arc::new(Mutex::new(env)),
        }
    }

    pub fn compile_spec(&mut self, spec: &ArcSpec) -> Result<(), failure::Error> {
        {
            let mut env = self.env.lock().unwrap();
            env.add_project(spec.id.clone())?;
            env.create_workspace_member(&env.root, &spec.id)?;
            env.generate(&env.root, &spec)?;
        }
        crate::util::cargo_build(&spec.mode)?;
        Ok(())
    }
}

impl Arconc for Server {
    fn compile(&mut self, _ctx: RpcContext, req: CompileRequest, _sink: UnarySink<CompileReply>) {
        match ArcSpec::from_bytes(&req.spec) {
            Ok(spec) => {
                debug!("Received Compilation Request {:?}", spec);
                match self.compile_spec(&spec) {
                    Ok(()) => {
                        debug!("Compiled {:?} successfully", spec.id);
                        // TODO: reply to gRPC sender with path to binary
                    }
                    Err(err) => {
                        error!("Failed to compile with err {:?}", err);
                        // TODO: reply to gRPC sender with failure reason
                    }
                }
            }
            Err(err) => {
                error!("Failed to parse specification with err {:?}", err);
                // TODO: reply to gRPC sender with failure reason
            }
        }
    }
}

pub fn start_server(
    host: &str,
    port: i32,
    compiler_env: CompilerEnv,
) {
    let env = Arc::new(Environment::new(num_cpus::get()));
    let server = Server::new(compiler_env);
    let service = create_arconc(server);
    let mut server = ServerBuilder::new(env)
        .register_service(service)
        .bind(host, port as u16)
        .build()
        .expect("Failed to set up arconc server");

    server.start();

    for &(ref host, port) in server.bind_addrs() {
        info!("listening on {}:{}", host, port);
    }

    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}
