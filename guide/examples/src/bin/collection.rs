use arcon::prelude::*;

fn main() {
    let mut pipeline = Pipeline::default()
        .with_debug_node()
        .collection((0..100).collect::<Vec<u64>>(), |conf| {
            conf.set_arcon_time(ArconTime::Event);
            conf.set_timestamp_extractor(|x: &u64| *x);
        })
        .operator(OperatorBuilder {
            constructor: Arc::new(|_| Filter::new(|x: &u64| *x > 50)),
            conf: Default::default(),
        })
        .operator(OperatorBuilder {
            constructor: Arc::new(|_| Map::new(|x| x + 10)),
            conf: Default::default(),
        })
        .build();

    pipeline.start();

    std::thread::sleep(std::time::Duration::from_millis(5000));
    let debug_node = pipeline.get_debug_node::<u64>().unwrap();

    debug_node.on_definition(|cd| {
        let sum: u64 = cd.data.iter().map(|elem| elem.data).sum();
        assert_eq!(sum, 4165);
    });
}
