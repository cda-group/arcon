use arcon::prelude::*;

fn main() {
    // Version 1 Timestamp extraction
    let mut pipeline = Pipeline::default()
        .file("file_source_data", |cfg| {
            cfg.set_arcon_time(ArconTime::Process);
        })
        .flatmap(|x: u64| (0..x))
        .to_console()
        .build();

    pipeline.start();
    pipeline.await_termination();
}
