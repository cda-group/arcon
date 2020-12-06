use arcon::prelude::*;

fn state_map(x: u64, _: &mut ()) -> OperatorResult<u64> {
    Ok(x)
}

fn main() {
    // TODO: Actually make this build the specified pipeline
    let pipeline = Pipeline::default()
        .collection((0..100).collect::<Vec<u64>>())
        .filter(Box::new(|x: &u64| *x > 50))
        .map_with_state(state_map, Box::new(|| ()), |conf| {
            conf.set_backend(BackendType::Sled);
            conf.set_state_id("map_state");
        })
        .build();

    pipeline.start();

    pipeline.await_termination();
}
