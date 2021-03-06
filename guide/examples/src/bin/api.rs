use arcon::prelude::*;

fn main() {
    let mut pipeline = Pipeline::default()
        .collection((0..100).collect::<Vec<u64>>(), |conf| {
            // ANCHOR: source_conf
            conf.set_arcon_time(ArconTime::Event);
            conf.set_timestamp_extractor(|x: &u64| *x);
            // ANCHOR_END: source_conf
        })
        // ANCHOR: operator
        .operator(OperatorBuilder {
            constructor: Arc::new(|_| Filter::new(|x| *x > 50)),
            conf: OperatorConf {
                parallelism_strategy: ParallelismStrategy::Static(1),
                ..Default::default()
            },
        })
        // ANCHOR_END: operator
        .to_console()
        .build();

    pipeline.start();
    pipeline.await_termination();
}
