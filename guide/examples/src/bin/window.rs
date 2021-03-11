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
        .operator(OperatorBuilder {
            constructor: Arc::new(|backend| {
                fn init(i: u64) -> u64 {
                    i
                }
                fn aggregation(i: u64, agg: &u64) -> u64 {
                    agg + i
                }

                let function = IncrementalWindow::new(backend.clone(), &init, &aggregation);
                WindowAssigner::sliding(
                    function,
                    backend,
                    Time::seconds(1000),
                    Time::seconds(500),
                    Time::seconds(0),
                    false,
                )
            }),
            conf: Default::default(),
        })
        .operator(OperatorBuilder {
            constructor: Arc::new(|backend| {
                let function = AppenderWindow::new(backend.clone(), &window_sum);
                WindowAssigner::tumbling(
                    function,
                    backend,
                    Time::seconds(2000),
                    Time::seconds(0),
                    false,
                )
            }),
            conf: Default::default(),
        })
        .to_console()
        .build();

    pipeline.start();
    pipeline.await_termination();
}
