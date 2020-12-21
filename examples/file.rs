use arcon::prelude::*;

fn main() {
    let mut pipeline = Pipeline::default()
        .file("file_source_data", |x| Some(*x))
        .flatmap(|x: u64| (0..x))
        .to_console()
        .build();

    pipeline.start();
    pipeline.await_termination();
}
