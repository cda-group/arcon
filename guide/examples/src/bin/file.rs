use arcon::prelude::*;

fn main() {
    let mut pipeline = Pipeline::default()
        .file("file_source_data", |cfg| {
            cfg.set_arcon_time(ArconTime::Process);
        })
        .flatmap(|x| (0..x))
        .to_console()
        .build();

    pipeline.start();
    pipeline.await_termination();
}
