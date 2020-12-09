use arcon::prelude::*;

fn main() {
    let pipeline = Pipeline::default()
        .collection((0..100).collect::<Vec<u64>>())
        .filter(|x: &u64| *x > 50)
        .map(|x: u64| x + 10)
        .build();

    pipeline.start();
    pipeline.await_termination();
}
