use arcon::prelude::*;

fn main() {
    let mut app = Application::default()
        .iterator(0u64..1000000000, |conf| {
            conf.set_arcon_time(ArconTime::Event);
            conf.set_timestamp_extractor(|x: &u64| *x);
        })
        .map_in_place(|m| *m += 1)
        .measure(10000000);

    app.start();
    app.await_termination();
}
