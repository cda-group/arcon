use arcon::prelude::*;

fn _window_sum(buffer: &[u64]) -> u64 {
    buffer.iter().sum()
}

fn main() {
    let mut pipeline = Pipeline::default()
        .collection((0..100000).collect::<Vec<u64>>(), |conf| {
            conf.set_arcon_time(ArconTime::Event);
            conf.set_timestamp_extractor(|x: &u64| *x);
        })
        .to_console()
        .build();

    pipeline.start();
    pipeline.await_termination();
}
