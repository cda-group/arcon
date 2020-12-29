use arcon::prelude::*;

fn main() {
    let mut pipeline = Pipeline::default()
        .collection((0..100).collect::<Vec<u64>>(), |conf| {
            conf.set_arcon_time(ArconTime::Event);
            conf.set_timestamp_extractor(|x: &u64| *x);
        })
        .filter(|x| *x > 50)
        .map(|x| x + 10)
        .to_console()
        .build();

    pipeline.start();
    pipeline.await_termination();
}
