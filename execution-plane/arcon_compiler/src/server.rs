use arcon_proto::arcon_spec::{spec_from_bytes, ArconSpec};
use arcon_proto::*;
use futures::*;
use grpcio::{Environment, RpcContext, ServerBuilder, UnarySink};

use std::sync::{Arc, Mutex};

use crate::env::CompilerEnv;

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

    pub fn compile_spec(&mut self, spec: &ArconSpec) -> Result<String, failure::Error> {
        let mut path = String::new();
        let mut logged: Option<String> = None;
        {
            let mut env = self.env.lock().unwrap();
            env.add_project(spec.id.clone())?;
            let features = env.generate(&spec)?;
            env.create_workspace_member(&spec.id, &features)?;

            if let Some(log_dir) = env.log_dir.clone() {
                logged = Some(log_dir)
            }

            let p: String = env.bin_path(&spec)?;
            path += &p;
        }

        crate::util::cargo_build(&spec, logged)?;
        Ok(path)
    }
}

impl Arconc for Server {
    fn compile(&mut self, ctx: RpcContext, req: ArconcRequest, sink: UnarySink<ArconcReply>) {
        let reply = match spec_from_bytes(&req.spec) {
            Ok(spec) => {
                debug!("Received Compilation Request {:?}", spec);
                match self.compile_spec(&spec) {
                    Ok(bin_path) => {
                        info!("Compiled {:?} successfully at path {}", spec.id, bin_path);
                        let mut success = ArconcSuccess::default();
                        success.set_path(bin_path);
                        let mut resp = ArconcReply::default();
                        resp.set_success(success);
                        resp
                    }
                    Err(err) => {
                        error!("Failed to compile with err {:?}", err);
                        let mut err_msg = ArconcError::default();
                        err_msg.set_message(err.to_string());
                        let mut reply = ArconcReply::default();
                        reply.set_error(err_msg);
                        reply
                    }
                }
            }
            Err(err) => {
                error!("Failed to parse specification with err {:?}", err);
                let mut err_msg = ArconcError::default();
                err_msg.set_message(err.to_string());
                let mut reply = ArconcReply::default();
                reply.set_error(err_msg);
                reply
            }
        };

        let f = sink
            .success(reply)
            .map_err(move |e| error!("failed to reply {:?}: {:?}", req, e));
        ctx.spawn(f)
    }
}

pub fn start_server(host: &str, port: i32, compiler_env: CompilerEnv) {
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
