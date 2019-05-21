extern crate codegen;
use codegen::prelude::*;
fn main() {
    let mut cfg = KompactConfig::new();
    let sock_addr = "127.0.0.1:3000"
        .parse()
        .expect("Failed to parse SocketAddr");
    cfg.system_components(DeadletterBox::new, NetworkConfig::new(sock_addr).build());
    let system = KompactSystem::new(cfg).expect("Failed to create KompactSystem");
}
