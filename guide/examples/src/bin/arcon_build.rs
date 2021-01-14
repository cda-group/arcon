use arcon::prelude::*;

// bring Event into scope
include!(concat!(env!("OUT_DIR"), "/event.rs"));

fn main() {
    let mut pipeline = Pipeline::default()
        .collection(
            (0..10000000)
                .map(|x| Event { id: x })
                .collect::<Vec<Event>>(),
            |conf| {
                conf.set_timestamp_extractor(|x: &Event| x.id);
            },
        )
        .operator(OperatorBuilder {
            constructor: Arc::new(|_| Filter::new(|event: &Event| event.id > 50)),
            conf: Default::default(),
        })
        .build();

    pipeline.start();
    pipeline.await_termination();
}
