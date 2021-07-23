use arcon::prelude::*;

// bring Event into scope
include!(concat!(env!("OUT_DIR"), "/event.rs"));

fn main() {
    let mut app = Application::default()
        .iterator((0..10000000).map(|x| Event { id: x }), |conf| {
            conf.set_timestamp_extractor(|x: &Event| x.id);
        })
        .filter(|event| event.id > 50)
        .build();

    app.start();
    app.await_termination();
}
