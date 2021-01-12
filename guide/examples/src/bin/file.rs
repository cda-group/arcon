use arcon::prelude::*;

fn main() {
    let mut pipeline = Pipeline::default()
        .file("file_source_data", |cfg| {
            cfg.set_arcon_time(ArconTime::Process);
        })
        .operator(OperatorBuilder {
            constructor: Arc::new(|_| FlatMap::new(|x| (0..x))),
            conf: Default::default(),
        })
        .to_console()
        .build();

    pipeline.start();
    pipeline.await_termination();
}
