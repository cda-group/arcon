use arcon::prelude::*;

fn window_sum(buffer: &[u64]) -> u64 {
    buffer.iter().sum()
}

fn main() {
    let mut pipeline = Pipeline::default()
        .collection((0..100000).collect::<Vec<u64>>(), |conf| {
            conf.set_arcon_time(ArconTime::Event);
            conf.set_timestamp_extractor(|x: &u64| *x);
        })
        .window(WindowBuilder {
            assigner: Assigner::Sliding {
                length: Time::seconds(1000),
                slide: Time::seconds(500),
                late_arrival: Time::seconds(0),
            },
            function: Arc::new(|backend: Arc<Sled>| {
                fn init(i: u64) -> u64 {
                    i
                }
                fn aggregation(i: u64, agg: &u64) -> u64 {
                    agg + i
                }
                IncrementalWindow::new(backend, &init, &aggregation)
            }),
            conf: Default::default(),
        })
        .window(WindowBuilder {
            assigner: Assigner::Tumbling {
                length: Time::seconds(2000),
                late_arrival: Time::seconds(0),
            },
            function: Arc::new(|backend: Arc<Sled>| AppenderWindow::new(backend, &window_sum)),
            conf: Default::default(),
        })
        .to_console()
        .build();

    pipeline.start();
    pipeline.await_termination();
}
