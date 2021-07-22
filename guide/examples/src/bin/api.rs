use arcon::prelude::*;

fn main() {
    let mut app = Application::default()
        .iterator(0u64..100, |conf| {
            // ANCHOR: source_conf
            conf.set_arcon_time(ArconTime::Event);
            conf.set_timestamp_extractor(|x: &u64| *x);
            // ANCHOR_END: source_conf
        })
        // ANCHOR: operator
        .operator(OperatorBuilder {
            constructor: Arc::new(|_: Arc<Sled>| Filter::new(|x| *x > 50)),
            conf: OperatorConf {
                parallelism_strategy: ParallelismStrategy::Static(1),
                ..Default::default()
            },
        })
        // ANCHOR_END: operator
        .to_console()
        .build();

    app.start();
    app.await_termination();
}
