use arcon::prelude::*;

fn main() {
    let mut app = Application::default()
        .with_debug_node()
        .iterator(0u64..100, |conf| {
            conf.set_arcon_time(ArconTime::Event);
            conf.set_timestamp_extractor(|x: &u64| *x);
        })
        .filter(|x| *x > 50)
        .map(|x| x + 10)
        .build();

    app.start();

    std::thread::sleep(std::time::Duration::from_millis(5000));
    let debug_node = app.get_debug_node::<u64>().unwrap();

    debug_node.on_definition(|cd| {
        let sum: u64 = cd.data.iter().map(|elem| elem.data).sum();
        assert_eq!(sum, 4165);
    });

    app.shutdown();
}
