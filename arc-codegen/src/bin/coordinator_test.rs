extern crate arc_codegen;

use arc_codegen::prelude::*;

fn main() {
    let system = KompactConfig::default().build().expect("KompactSystem");
    /*
    let operator = system.create(Operator::new);
    system.start(&operator);

    let tcp_source = system.create_and_register(move || TcpSource::new(operator.actor_ref()));
    system.start(&tcp_source.0);
    */
    system.await_termination();
}
